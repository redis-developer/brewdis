package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.DELTA;
import static com.redislabs.demos.retail.Field.SKU;
import static com.redislabs.demos.retail.Field.STORE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.TaskExecutor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.springframework.stereotype.Component;

import lombok.Setter;

@Component
public class InventoryGenerator implements InitializingBean, Runnable {

	@Autowired
	private RetailConfig config;
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	private TaskExecutor executor = new ConcurrentTaskExecutor();
	@Setter
	private List<String> skus = new ArrayList<>();
	private Random random = new Random();
	private OfInt stores;
	private OfInt deltas;

	@Override
	public void afterPropertiesSet() throws Exception {
		stores = random.ints(1, 4).iterator();
		deltas = random.ints(config.getGeneratorDeltaMin(), config.getGeneratorDeltaMax()).iterator();
		executor.execute(this);
	}

	@Override
	public void run() {
		while (true) {
			skus.forEach(sku -> {
				redisTemplate.opsForStream().add(config.getInventoryUpdatesStream(), fields(sku));
				try {
					Thread.sleep(config.getGeneratorSleep());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			});
		}

	}

	private Map<String, String> fields(String sku) {
		String store = String.valueOf(stores.nextInt());
		String delta = String.valueOf(deltas.nextInt());
		Map<String, String> fields = new HashMap<>();
		fields.put(STORE, store);
		fields.put(SKU, sku);
		fields.put(DELTA, delta);
		return fields;
	}

}
