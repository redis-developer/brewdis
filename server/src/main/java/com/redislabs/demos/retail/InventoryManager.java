package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.ALLOCATED;
import static com.redislabs.demos.retail.Field.AVAILABLE_TO_PROMISE;
import static com.redislabs.demos.retail.Field.DELTA;
import static com.redislabs.demos.retail.Field.EPOCH;
import static com.redislabs.demos.retail.Field.LEVEL;
import static com.redislabs.demos.retail.Field.ON_HAND;
import static com.redislabs.demos.retail.Field.RESERVED;
import static com.redislabs.demos.retail.Field.SKU;
import static com.redislabs.demos.retail.Field.STORE;
import static com.redislabs.demos.retail.Field.TIME;
import static com.redislabs.demos.retail.Field.VIRTUAL_HOLD;

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
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

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
	private StringRedisTemplate template;
	private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private Subscription subscription;

	@Override
	public void afterPropertiesSet() throws Exception {
		this.container = StreamMessageListenerContainer.create(template.getConnectionFactory(),
				StreamMessageListenerContainerOptions.builder().pollTimeout(Duration.ofMillis(10000)).build());
		container.start();
		this.subscription = container.receive(StreamOffset.fromStart(config.getInventory().getInputStream()), this);
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
		String store = message.getValue().get(STORE);
		String sku = message.getValue().get(SKU);
		String id = config.key(store, sku);
		String docId = config.key(config.getInventory().getKeyspace(), id);
		Map<String, String> inventory = connection.sync().get(config.getInventory().getIndex(), docId);
		if (message.getValue().containsKey(ON_HAND)) {
			int delta = Integer.parseInt(message.getValue().get(ON_HAND));
			log.info("Received restocking for {}:{} delta={}", store, sku, delta);
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
		template.opsForStream().add(config.getInventory().getOutputStream(), inventory);
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

}