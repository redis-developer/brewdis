package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.ALLOCATED;
import static com.redislabs.demos.retail.Field.AVAILABLE_TO_PROMISE;
import static com.redislabs.demos.retail.Field.DELTA;
import static com.redislabs.demos.retail.Field.DESCRIPTION;
import static com.redislabs.demos.retail.Field.EPOCH;
import static com.redislabs.demos.retail.Field.ID;
import static com.redislabs.demos.retail.Field.LEVEL;
import static com.redislabs.demos.retail.Field.ON_HAND;
import static com.redislabs.demos.retail.Field.RESERVED;
import static com.redislabs.demos.retail.Field.SKU;
import static com.redislabs.demos.retail.Field.STORE;
import static com.redislabs.demos.retail.Field.STORE_DESCRIPTION;
import static com.redislabs.demos.retail.Field.TIME;
import static com.redislabs.demos.retail.Field.VIRTUAL_HOLD;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;

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

import com.redislabs.demos.retail.RetailConfig.InventoryGeneratorConfig;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryManager
		implements InitializingBean, DisposableBean, StreamListener<String, MapRecord<String, String, String>> {

	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();
	private Random random = new Random();
	private OfInt onHands;
	private OfInt allocateds;
	private OfInt reserveds;
	private OfInt virtualHolds;

	@Autowired
	private RetailConfig config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private StringRedisTemplate template;
	private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private Subscription subscription;

	@Override
	public void afterPropertiesSet() throws Exception {
		InventoryGeneratorConfig generatorConfig = this.config.getInventory().getGenerator();
		this.onHands = random.ints(generatorConfig.getOnHandMin(), generatorConfig.getOnHandMax()).iterator();
		this.allocateds = random.ints(generatorConfig.getAllocatedMin(), generatorConfig.getAllocatedMax()).iterator();
		this.reserveds = random.ints(generatorConfig.getReservedMin(), generatorConfig.getReservedMax()).iterator();
		this.virtualHolds = random.ints(generatorConfig.getVirtualHoldMin(), generatorConfig.getVirtualHoldMax())
				.iterator();
		this.container = StreamMessageListenerContainer.create(template.getConnectionFactory(),
				StreamMessageListenerContainerOptions.builder().pollTimeout(Duration.ofMillis(10000)).build());
		container.start();
		this.subscription = container.receive(StreamOffset.latest(config.getInventory().getInputStream()), this);
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
		int delta = Integer.parseInt(message.getValue().get(DELTA));
		RediSearchCommands<String, String> commands = connection.sync();
		String productDocId = key(config.getProduct().getKeyspace(), sku);
		Map<String, String> productDoc = commands.get(config.getProduct().getIndex(), productDocId);
		if (productDoc == null) {
			log.warn("Unknown product {}", productDocId);
			return;
		}
		String storeDocId = key(config.getStore().getKeyspace(), store);
		Map<String, String> storeDoc = commands.get(config.getStore().getIndex(), storeDocId);
		if (storeDoc == null) {
			log.warn("Unknown store {}", storeDocId);
			return;
		}
		String storeDescription = storeDoc.remove(DESCRIPTION);
		storeDoc.put(STORE_DESCRIPTION, storeDescription);
		String id = key(store, sku);
		String docId = key(config.getInventory().getKeyspace(), id);
		Map<String, String> inventory = commands.get(config.getInventory().getIndex(), docId);
		if (inventory == null) {
			inventory = new HashMap<>();
			inventory.putAll(productDoc);
			inventory.putAll(storeDoc);
			inventory.put(ON_HAND, String.valueOf(onHands.nextInt()));
			inventory.put(ALLOCATED, String.valueOf(allocateds.nextInt()));
			inventory.put(RESERVED, String.valueOf(reserveds.nextInt()));
			inventory.put(VIRTUAL_HOLD, String.valueOf(virtualHolds.nextInt()));
			inventory.put(ID, id);
		}
		int allocated = Integer.parseInt(inventory.get(ALLOCATED));
		int reserved = Integer.parseInt(inventory.get(RESERVED));
		int virtualHold = Integer.parseInt(inventory.get(VIRTUAL_HOLD));
		int demand = allocated + reserved + virtualHold;
		int previousOnHand = Integer.parseInt(inventory.get(ON_HAND));
		int newOnHand = previousOnHand + delta;
		if (newOnHand < 0) {
			newOnHand = 0;
		}
		int availableToPromise = newOnHand - demand;
		if (availableToPromise < 0) {
			availableToPromise = 0;
			newOnHand = demand;
		}
		inventory.put(ON_HAND, String.valueOf(newOnHand));
		inventory.put(AVAILABLE_TO_PROMISE, String.valueOf(availableToPromise));
		ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);
		inventory.put(TIME, time.format(DateTimeFormatter.ISO_INSTANT));
		inventory.put(EPOCH, String.valueOf(time.toEpochSecond()));
		inventory.put(DELTA, String.valueOf(delta));
		inventory.put(LEVEL, config.getInventory().level(availableToPromise));
		template.opsForStream().add(config.getInventory().getOutputStream(), inventory);
		try {
			commands.add(config.getInventory().getIndex(), docId, 1.0, inventory, addOptions);
		} catch (RedisCommandExecutionException e) {
			log.error("Could not add document {}: {}", docId, inventory, e);
		}
	}

	private String key(String... keys) {
		return String.join(config.getKeySeparator(), keys);
	}

}