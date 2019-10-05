package com.redislabs.demos.retail;

import java.time.Duration;
import java.util.Map;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.demos.retail.model.Inventory;
import com.redislabs.demos.retail.model.InventoryUpdate;
import com.redislabs.demos.retail.model.Product;
import com.redislabs.demos.retail.model.Store;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryUpdateListener
		implements StreamListener<String, MapRecord<String, String, String>>, InitializingBean {

	@Autowired
	private InventoryUpdateListener inventoryUpdateListener;
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
	private ObjectMapper mapper = new ObjectMapper();
	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();

	@Override
	public void afterPropertiesSet() throws Exception {
		StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> containerOptions = StreamMessageListenerContainerOptions
				.builder().pollTimeout(Duration.ofMillis(1000)).build();
		StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
				.create(redisTemplate.getConnectionFactory(), containerOptions);
		container.start();
		Subscription subscription = container.receive(StreamOffset.latest(config.getInventoryUpdatesStream()),
				inventoryUpdateListener);
		subscription.await(Duration.ofSeconds(2));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onMessage(MapRecord<String, String, String> message) {
		InventoryUpdate inventoryUpdate = mapper.convertValue(message.getValue(), InventoryUpdate.class);
		RediSearchCommands<String, String> commands = connection.sync();
		String productDocId = utils.key(config.getProductKeyspace(), inventoryUpdate.getSku());
		Map<String, String> productDoc = commands.get(config.getProductIndex(), productDocId);
		if (productDoc == null) {
			log.warn("Unknown product {}", productDocId);
			return;
		}
		Product product = mapper.convertValue(productDoc, Product.class);
		String storeDocId = utils.key(config.getStoreKeyspace(), inventoryUpdate.getStore());
		Map<String, String> storeDoc = commands.get(config.getStoreIndex(), storeDocId);
		if (storeDoc == null) {
			log.warn("Unknown store {}", storeDocId);
			return;
		}
		Store store = mapper.convertValue(storeDoc, Store.class);
		String docId = utils.key(config.getInventoryKeyspace(), store.getId(), product.getSku());
		Map<String, String> doc = commands.get(config.getInventoryIndex(), docId);
		Inventory inventory;
		if (doc == null) {
			inventory = Inventory.builder().abv(product.getAbv()).address(store.getAddress())
					.address2(store.getAddress2()).address3(store.getAddress3())
					.availableToSell(store.getAvailableToSell()).city(store.getCity()).country(store.getCountry())
					.isDefaultStore(store.getIsDefault()).isPreferredStore(store.getIsPreferred())
					.latitude(store.getLatitude()).location(store.getLocation()).longitude(store.getLongitude())
					.market(store.getMarket()).organic(product.getOrganic()).parentDc(store.getParentDc())
					.productCategory(product.getCategory()).productDescription(product.getDescription())
					.productName(product.getName()).productStyle(product.getStyle())
					.rollupInventory(store.getRollupInventory()).sku(product.getSku()).state(store.getState())
					.store(store.getId()).storeDescription(store.getDescription()).storeType(store.getType())
					.zip(store.getZip()).build();
		} else {
			inventory = mapper.convertValue(doc, Inventory.class);
		}
		if (inventory.getQuantity() == null) {
			inventory.setQuantity("0");
		}
		inventory
				.setQuantity(String.valueOf(Integer.parseInt(inventory.getQuantity()) + inventoryUpdate.getQuantity()));
		Map<String, String> fields = mapper.convertValue(inventory, Map.class);
		try {
			commands.add(config.getInventoryIndex(), docId, 1.0, fields, addOptions);
		} catch (RedisCommandExecutionException e) {
			log.error("Could not add document {}: {}", docId, fields, e);
		}
		sendingOps.convertAndSend(config.getStomp().getInventoryTopic(), inventory);
	}

}
