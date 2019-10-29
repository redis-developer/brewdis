package com.redislabs.demo.brewdis;

import static com.redislabs.demo.brewdis.Field.ALLOCATED;
import static com.redislabs.demo.brewdis.Field.AVAILABLE_TO_PROMISE;
import static com.redislabs.demo.brewdis.Field.DELTA;
import static com.redislabs.demo.brewdis.Field.EPOCH;
import static com.redislabs.demo.brewdis.Field.LEVEL;
import static com.redislabs.demo.brewdis.Field.LOCATION;
import static com.redislabs.demo.brewdis.Field.ON_HAND;
import static com.redislabs.demo.brewdis.Field.PRODUCT_ID;
import static com.redislabs.demo.brewdis.Field.RESERVED;
import static com.redislabs.demo.brewdis.Field.STORE_ID;
import static com.redislabs.demo.brewdis.Field.TIME;
import static com.redislabs.demo.brewdis.Field.VIRTUAL_HOLD;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.DropOptions;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.TagField;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryManager
		implements InitializingBean, DisposableBean, StreamListener<String, MapRecord<String, String, String>> {

	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();

	@Autowired
	private BrewdisConfig config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private StringRedisTemplate redis;
	private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private Subscription subscription;

	@Override
	public void afterPropertiesSet() throws Exception {
		String index = config.getInventory().getIndex();
		log.info("Dropping {} index", index);
		try {
			connection.sync().drop(index, DropOptions.builder().build());
		} catch (RedisCommandExecutionException e) {
			if (!e.getMessage().equals("Unknown Index name")) {
				throw e;
			}
		}
		log.info("Creating {} index", index);
		Schema schema = Schema.builder().field(TagField.builder().name(STORE_ID).sortable(true).build())
				.field(TagField.builder().name(PRODUCT_ID).sortable(true).build())
				.field(GeoField.builder().name(LOCATION).build())
				.field(NumericField.builder().name(AVAILABLE_TO_PROMISE).sortable(true).build())
				.field(NumericField.builder().name(ON_HAND).sortable(true).build())
				.field(NumericField.builder().name(ALLOCATED).sortable(true).build())
				.field(NumericField.builder().name(RESERVED).sortable(true).build())
				.field(NumericField.builder().name(VIRTUAL_HOLD).sortable(true).build())
				.field(NumericField.builder().name(EPOCH).sortable(true).build()).build();
		connection.sync().create(index, schema);
		this.container = StreamMessageListenerContainer.create(redis.getConnectionFactory(),
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofMillis(config.getStreamPollTimeout())).build());
		container.start();
		this.subscription = container.receive(StreamOffset.fromStart(config.getInventory().getGenerator().getStream()),
				this);
		subscription.await(Duration.ofSeconds(2));
	}

	@Override
	public void destroy() throws Exception {
		if (subscription != null) {
			subscription.cancel();
		}
		if (container != null) {
			container.stop();
		}
	}

	@Override
	public void onMessage(MapRecord<String, String, String> message) {
		String store = message.getValue().get(STORE_ID);
		String sku = message.getValue().get(PRODUCT_ID);
		String id = config.concat(store, sku);
		String docId = config.concat(config.getInventory().getKeyspace(), id);
		Map<String, String> inventory = connection.sync().get(config.getInventory().getIndex(), docId);
		if (message.getValue().containsKey(ON_HAND)) {
			int delta = Integer.parseInt(message.getValue().get(ON_HAND));
			log.info("Received restocking for {}:{} {}={}", store, sku, DELTA, delta);
			int previousOnHand = Integer.parseInt(inventory.get(ON_HAND));
			int onHand = previousOnHand + delta;
			inventory.put(ON_HAND, String.valueOf(onHand));
		}
		if (message.getValue().containsKey(ALLOCATED)) {
			int delta = Integer.parseInt(message.getValue().get(ALLOCATED));
			int previousAllocated = Integer.parseInt(inventory.get(ALLOCATED));
			int allocated = previousAllocated + delta;
			inventory.put(ALLOCATED, String.valueOf(allocated));
		}
		int availableToPromise = availableToPromise(inventory);
		if (availableToPromise < 0) {
			return;
		}
		int delta = 0;
		if (inventory.containsKey(AVAILABLE_TO_PROMISE)) {
			int previousAvailableToPromise = Integer.parseInt(inventory.get(AVAILABLE_TO_PROMISE));
			delta = availableToPromise - previousAvailableToPromise;
		}
		inventory.put(DELTA, String.valueOf(delta));
		inventory.put(AVAILABLE_TO_PROMISE, String.valueOf(availableToPromise));
		ZonedDateTime time = ZonedDateTime.now();
		inventory.put(TIME, time.format(DateTimeFormatter.ISO_INSTANT));
		inventory.put(EPOCH, String.valueOf(time.toEpochSecond()));
		inventory.put(LEVEL, config.getInventory().level(availableToPromise));
		redis.opsForStream().add(config.getInventory().getStream(), inventory);
		try {
			connection.sync().add(config.getInventory().getIndex(), docId, 1.0, inventory, addOptions);
		} catch (RedisCommandExecutionException e) {
			log.error("Could not add document {}: {}", docId, inventory, e);
		}
	}

	private int availableToPromise(Map<String, String> inventory) {
		int allocated = Integer.parseInt(inventory.get(ALLOCATED));
		int reserved = Integer.parseInt(inventory.get(RESERVED));
		int virtualHold = Integer.parseInt(inventory.get(VIRTUAL_HOLD));
		int demand = allocated + reserved + virtualHold;
		int supply = Integer.parseInt(inventory.get(ON_HAND));
		return supply - demand;
	}

	@Scheduled(fixedRateString = "${inventory.cleanup.rate}")
	public void cleanup() {
		ZonedDateTime time = ZonedDateTime.now()
				.minus(Duration.ofSeconds(config.getInventory().getCleanup().getAgeThreshold()));
		String query = "@" + EPOCH + ":[0 " + time.toEpochSecond() + "]";
		String index = config.getInventory().getIndex();
		SearchResults<String, String> results = connection.sync().search(index, query,
				SearchOptions.builder().noContent(true)
						.limit(Limit.builder().num(config.getInventory().getCleanup().getSearchLimit()).build())
						.build());
		results.forEach(r -> connection.sync().del(index, r.getDocumentId(), true));
		if (results.size() > 0) {
			log.info("Deleted {} docs from {} index", results.size(), config.getInventory().getIndex());
		}
		redis.opsForStream().trim(config.getInventory().getGenerator().getStream(),
				config.getInventory().getCleanup().getStreamTrimCount());
		redis.opsForStream().trim(config.getInventory().getStream(),
				config.getInventory().getCleanup().getStreamTrimCount());
	}

}