package com.redislabs.demo.brewdis;

import static com.redislabs.demo.brewdis.BrewdisField.AVAILABLE_TO_PROMISE;
import static com.redislabs.demo.brewdis.BrewdisField.ON_HAND;
import static com.redislabs.demo.brewdis.BrewdisField.PRODUCT_ID;
import static com.redislabs.demo.brewdis.BrewdisField.STORE_ID;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventorySupply
		implements InitializingBean, DisposableBean, StreamListener<String, MapRecord<String, String, String>> {

	@Autowired
	private Config config;
	@Autowired
	private StringRedisTemplate template;
	private ScheduledExecutorService executor;
	private OfInt delays;
	private Subscription subscription;
	private OfInt restockOnHands;
	private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private Map<String, ZonedDateTime> scheduledRestocks = new HashMap<>();

	@Override
	public void afterPropertiesSet() throws Exception {
		Random random = new Random();
		this.delays = random.ints(config.getInventory().getRestock().getDelayMin(),
				config.getInventory().getRestock().getDelayMax()).iterator();
		this.restockOnHands = random.ints(config.getInventory().getRestock().getDeltaMin(),
				config.getInventory().getRestock().getDeltaMax()).iterator();
		this.container = StreamMessageListenerContainer.create(template.getConnectionFactory(),
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofMillis(config.getStreamPollTimeout())).build());
		container.start();
		this.subscription = container.receive(StreamOffset.latest(config.getInventory().getStream()), this);
		subscription.await(Duration.ofSeconds(2));
		executor = Executors.newSingleThreadScheduledExecutor();
	}

	@Override
	public void destroy() throws Exception {
		if (executor != null) {
			executor.shutdownNow();
		}
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
		int available = Integer.parseInt(message.getValue().get(AVAILABLE_TO_PROMISE));
		if (available < config.getInventory().getRestock().getThreshold()) {
			String id = config.concat(store, sku);
			if (scheduledRestocks.containsKey(id)) {
				return;
			}
			scheduledRestocks.put(id, ZonedDateTime.now());
			int delay = delays.nextInt();
			log.info("Scheduling restocking for {}:{} in {} seconds", store, sku, delay);
			executor.schedule(new Runnable() {

				@Override
				public void run() {
					int delta = restockOnHands.nextInt();
					Map<String,String> message = new HashMap<>();
					message.put(STORE_ID, store);
					message.put(PRODUCT_ID, sku);
					message.put(ON_HAND, String.valueOf(delta));
					template.opsForStream().add(config.getInventory().getUpdateStream(), message);
					scheduledRestocks.remove(id);
				}
			}, delay, TimeUnit.SECONDS);
		}
	}

}
