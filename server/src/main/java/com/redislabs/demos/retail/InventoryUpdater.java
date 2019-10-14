package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.DELTA;
import static com.redislabs.demos.retail.Field.QUANTITY;
import static com.redislabs.demos.retail.Field.SKU;
import static com.redislabs.demos.retail.Field.STORE;
import static com.redislabs.demos.retail.Field.TIME;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryUpdater
		implements StreamListener<String, MapRecord<String, String, String>>, InitializingBean, DisposableBean {

	@Autowired
	private RetailConfig config;
	@Autowired
	private RedisUtils utils;
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private SimpMessageSendingOperations sendingOps;

	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();
	private Subscription subscription;
	private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	private Random random = new Random();

	@Override
	public void afterPropertiesSet() throws Exception {
		container = StreamMessageListenerContainer.create(redisTemplate.getConnectionFactory(),
				StreamMessageListenerContainerOptions.builder().pollTimeout(Duration.ofMillis(100)).build());
		container.start();
		subscription = container.receive(StreamOffset.latest(config.getInventoryUpdatesStream()), this);
		subscription.await(Duration.ofSeconds(2));
	}

	@Override
	public void destroy() throws Exception {
		log.info("Cancelling inventory subscription");
		subscription.cancel();
		log.info("Stopping inventory listener container");
		container.stop();
	}

	@Override
	public void onMessage(MapRecord<String, String, String> message) {
		Map<String, String> inventoryUpdate = message.getValue();
		String store = inventoryUpdate.get(STORE);
		String sku = inventoryUpdate.get(SKU);
		RediSearchCommands<String, String> commands = connection.sync();
		String productDocId = utils.key(config.getProductKeyspace(), sku);
		Map<String, String> productDoc = commands.get(config.getProductIndex(), productDocId);
		if (productDoc == null) {
			log.warn("Unknown product {}", productDocId);
			return;
		}
		String storeDocId = utils.key(config.getStoreKeyspace(), store);
		Map<String, String> storeDoc = commands.get(config.getStoreIndex(), storeDocId);
		if (storeDoc == null) {
			log.warn("Unknown store {}", storeDocId);
			return;
		}
		String id = utils.key(store, sku);
		String docId = utils.key(config.getInventoryKeyspace(), id);
		Map<String, String> inventory = commands.get(config.getInventoryIndex(), docId);
		if (inventory == null) {
			inventory = new HashMap<>();
			inventory.putAll(productDoc);
			inventory.putAll(storeDoc);
			inventory.put(Field.QUANTITY, String.valueOf(config.getInventoryRestockingQuantity()));
			inventory.put(Field.ID, id);
		}
		int delta = Integer.parseInt(inventoryUpdate.get(DELTA));
		int oldQuantity = Integer.parseInt(inventory.get(QUANTITY));
		int newQuantity = oldQuantity + delta;
		if (newQuantity < 0) {
			newQuantity = 0;
		}
		if (newQuantity == 0) {
			executor.schedule(new Runnable() {

				@Override
				public void run() {
					int delta = random.nextInt(config.getInventoryRestockingQuantity());
					redisTemplate.opsForStream().add(config.getInventoryUpdatesStream(),
							Map.of(STORE, store, SKU, sku, DELTA, String.valueOf(delta)));
				}
			}, random.nextInt(config.getInventoryRestockingDelay()), TimeUnit.SECONDS);
		}
		if (newQuantity == oldQuantity) {
			return;
		}
		inventory.put(QUANTITY, String.valueOf(newQuantity));
		inventory.put(TIME, ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
		inventory.put(DELTA, String.valueOf(delta));
		sendingOps.convertAndSend(config.getStomp().getInventoryTopic(), inventory);
		inventory.remove(DELTA);
		try {
			commands.add(config.getInventoryIndex(), docId, 1.0, inventory, addOptions);
		} catch (RedisCommandExecutionException e) {
			log.error("Could not add document {}: {}", docId, inventory, e);
		}
	}

}
