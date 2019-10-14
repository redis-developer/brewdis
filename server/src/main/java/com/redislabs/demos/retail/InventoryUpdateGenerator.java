package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.DELTA;
import static com.redislabs.demos.retail.Field.SKU;
import static com.redislabs.demos.retail.Field.STORE;

import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class InventoryUpdateGenerator implements InitializingBean, DisposableBean {

	@Autowired
	private RetailConfig config;
	@Autowired
	private RedisTemplate<String, String> template;
	private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	private Random random = new Random();
	private OfInt deltas;

	@Override
	public void afterPropertiesSet() throws Exception {
		this.deltas = random.ints(config.getInventoryDeltaMin(), config.getInventoryDeltaMax()).iterator();
	}

	public void generate(List<String> skus, List<String> stores) {
		if (stores.isEmpty()) {
			return;
		}
		if (skus.isEmpty()) {
			return;
		}
		ScheduledFuture<?> future = executor.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				template.opsForStream().add(config.getInventoryUpdatesStream(),
						Map.of(STORE, stores.get(random.nextInt(stores.size())), SKU,
								skus.get(random.nextInt(skus.size())), DELTA, String.valueOf(deltas.nextInt())));
			}
		}, 0, config.getGeneratorSleep(), TimeUnit.MILLISECONDS);
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				future.cancel(true);
			}
		}, 10, TimeUnit.MINUTES);
	}

	public void destroy() throws Exception {
		executor.shutdownNow();
	}

}
