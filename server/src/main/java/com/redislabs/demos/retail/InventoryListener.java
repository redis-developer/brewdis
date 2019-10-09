package com.redislabs.demos.retail;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import com.redislabs.demos.retail.model.Field;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryListener implements StreamListener<String, MapRecord<String, String, String>>, InitializingBean {

	@Autowired
	private RetailConfig config;
	@Autowired
	private RedisUtils utils;
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();
	private Subscription subscription;

	@Override
	public void afterPropertiesSet() throws Exception {
		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> containerOptions = StreamMessageListenerContainerOptions
				.builder().pollTimeout(Duration.ofMillis(1000)).build();
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(redisTemplate.getConnectionFactory(), containerOptions);
		container.start();
		subscription = container.receive(StreamOffset.latest(config.getInventoryUpdatesStream()), this);
		subscription.await(Duration.ofSeconds(2));
	}

	@PreDestroy
	public void teardown() {
		subscription.cancel();
	}

	@Override
	public void onMessage(MapRecord<String, String, String> message) {
		Map<String, String> inventoryUpdate = message.getValue();
		String store = inventoryUpdate.get(Field.store.name());
		String sku = inventoryUpdate.get(Field.sku.name());
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
		String docId = utils.key(config.getInventoryKeyspace(), store, sku);
		Map<String, String> inventory = commands.get(config.getInventoryIndex(), docId);
		if (inventory == null) {
			inventory = new HashMap<>();
			inventory.putAll(productDoc);
			inventory.putAll(storeDoc);
			inventory.put(Field.quantity.name(), "100");
		}
		int quantity = Integer.parseInt(inventory.get(Field.quantity.name()));
		int updateQuantity = Integer.parseInt(inventoryUpdate.get(Field.quantity.name()));
		inventory.put(Field.delta.name(), String.valueOf(updateQuantity));
		inventory.put(Field.quantity.name(), String.valueOf(Math.max(quantity + updateQuantity, 0)));
		try {
			commands.add(config.getInventoryIndex(), docId, 1.0, inventory, addOptions);
		} catch (RedisCommandExecutionException e) {
			log.error("Could not add document {}: {}", docId, inventory, e);
		}
		String streamKey = utils.key(config.getInventoryUpdatesStream(), store, sku);
		commands.xadd(streamKey, inventory);
	}

}
