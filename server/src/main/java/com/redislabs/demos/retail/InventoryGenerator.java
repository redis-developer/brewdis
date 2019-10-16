package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.DELTA;
import static com.redislabs.demos.retail.Field.SKU;
import static com.redislabs.demos.retail.Field.STORE;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.extern.slf4j.Slf4j;
import lombok.Data;
import lombok.ToString;

@Component
@Slf4j
public class InventoryGenerator implements InitializingBean {

	@Autowired
	private RetailConfig config;
	@Autowired
	private RedisTemplate<String, String> template;
	private Random random = new Random();
	private OfInt deltas;
	private List<Request> requests = new ArrayList<>();

	@Data
	@Builder
	@ToString
	static private class Request {
		@Default
		private ZonedDateTime time = ZonedDateTime.now();
		private List<String> skus;
		private List<String> stores;

	}

	public void request(List<String> stores, List<String> skus) {
		synchronized (requests) {
			Request request = Request.builder().stores(stores).skus(skus).build();
			log.info("Adding generator request: {}", request);
			requests.add(request);
		}
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.deltas = random.ints(config.getInventory().getGenerator().getDeltaMin(),
				config.getInventory().getGenerator().getDeltaMax()).iterator();
	}

	@Scheduled(fixedRate = 100)
	public void generate() {
		synchronized (requests) {
			List<Request> expired = new ArrayList<>();
			for (Request request : requests) {
				if (request.getTime().isBefore(ZonedDateTime.now().minus(Duration
						.of(config.getInventory().getGenerator().getRequestDurationInMin(), ChronoUnit.MINUTES)))) {
					expired.add(request);
					log.info("Request expired: {}", request);
				}
			}
			requests.removeAll(expired);
			for (Request request : requests) {
				if (request.getStores().isEmpty() || request.getSkus().isEmpty()) {
					return;
				}
				String store = request.getStores().get(random.nextInt(request.getStores().size()));
				String sku = request.getSkus().get(random.nextInt(request.getSkus().size()));
				template.opsForStream().add(config.getInventory().getInputStream(),
						Map.of(STORE, store, SKU, sku, DELTA, String.valueOf(deltas.nextInt())));
			}
		}
	}

}
