package com.redislabs.demo.brewdis;

import static com.redislabs.demo.brewdis.Field.ALLOCATED;
import static com.redislabs.demo.brewdis.Field.ON_HAND;
import static com.redislabs.demo.brewdis.Field.PRODUCT_ID;
import static com.redislabs.demo.brewdis.Field.RESERVED;
import static com.redislabs.demo.brewdis.Field.STORE_ID;
import static com.redislabs.demo.brewdis.Field.VIRTUAL_HOLD;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.redislabs.demo.brewdis.BrewdisConfig.InventoryGeneratorConfig;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryGenerator implements InitializingBean {

	@Data
	@AllArgsConstructor
	public class GeneratorRequest {
		private ZonedDateTime time;
		private String requester;
		private List<StoreSku> storeSkus = new ArrayList<>();

		public boolean isExpired() {
			long ageInMinutes = ChronoUnit.MINUTES.between(time, ZonedDateTime.now());
			return ageInMinutes > config.getInventory().getGenerator().getRequestDurationInMin();
		}
	}

	@Data
	@AllArgsConstructor
	public static class StoreSku {
		private String store;
		private String sku;
	}

	@Autowired
	private BrewdisConfig config;
	@Autowired
	private StringRedisTemplate redis;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	private Random random = new Random();
	private OfInt deltas;
	private List<GeneratorRequest> requests = new ArrayList<>();
	private OfInt onHands;
	private OfInt allocateds;
	private OfInt reserveds;
	private OfInt virtualHolds;
	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();

	@Override
	public void afterPropertiesSet() throws Exception {
		InventoryGeneratorConfig generatorConfig = config.getInventory().getGenerator();
		this.deltas = random.ints(generatorConfig.getDeltaMin(), generatorConfig.getDeltaMax()).iterator();
		this.onHands = random.ints(generatorConfig.getOnHandMin(), generatorConfig.getOnHandMax()).iterator();
		this.allocateds = random.ints(generatorConfig.getAllocatedMin(), generatorConfig.getAllocatedMax()).iterator();
		this.reserveds = random.ints(generatorConfig.getReservedMin(), generatorConfig.getReservedMax()).iterator();
		this.virtualHolds = random.ints(generatorConfig.getVirtualHoldMin(), generatorConfig.getVirtualHoldMax())
				.iterator();
		redis.delete(config.getInventory().getUpdateStream());
	}

	@Scheduled(fixedRateString = "${inventory.generator.rate}")
	public void generate() {
		synchronized (requests) {
			requests.forEach(r -> {
				StoreSku s = r.getStoreSkus().get(random.nextInt(r.getStoreSkus().size()));
				Map<String, String> update = new HashMap<>();
				update.put(STORE_ID, s.getStore());
				update.put(PRODUCT_ID, s.getSku());
				update.put(ALLOCATED, String.valueOf(deltas.nextInt()));
				redis.opsForStream().add(config.getInventory().getUpdateStream(), update);
			});
		}
	}

	@Scheduled(fixedRate = 60000)
	public void pruneRequests() {
		synchronized (requests) {
			requests.removeIf(r -> r.isExpired());
		}
	}

	public void add(String requester, List<String> stores, List<String> skus) {
		if (stores.isEmpty()) {
			return;
		}
		if (skus.isEmpty()) {
			return;
		}
		log.info("Adding stores {} and skus {}", stores, skus);
		List<StoreSku> storeSkus = new ArrayList<>();
		for (int index = 0; index < skus.size(); index++) {
			String sku = skus.get(index);
			for (int i = 0; i < config.getInventory().getGenerator().getStoresPerSku(); i++) {
				String store = stores.get((index + i) % stores.size());
				storeSkus.add(new StoreSku(store, sku));
			}
		}
		storeSkus.forEach(s -> {
			String store = s.getStore();
			String sku = s.getSku();
			RediSearchCommands<String, String> commands = connection.sync();
			String docId = config.concat(config.getInventory().getKeyspace(), store, sku);
			if (commands.get(config.getInventory().getIndex(), docId) == null) {
				Map<String, String> productDoc = commands.get(config.getProduct().getIndex(),
						config.concat(config.getProduct().getKeyspace(), sku));
				if (productDoc == null) {
					log.warn("Unknown product '{}'", sku);
					return;
				}
				Map<String, String> storeDoc = commands.get(config.getStore().getIndex(),
						config.concat(config.getStore().getKeyspace(), store));
				if (storeDoc == null) {
					log.warn("Unknown store '{}'", store);
					return;
				}
				Map<String, String> inventory = new HashMap<>();
				config.getProduct().getInventoryMapping().forEach((k, v) -> inventory.put(v, productDoc.get(k)));
				config.getStore().getInventoryMapping().forEach((k, v) -> inventory.put(v, storeDoc.get(k)));
				inventory.put(ON_HAND, String.valueOf(onHands.nextInt()));
				inventory.put(ALLOCATED, String.valueOf(allocateds.nextInt()));
				inventory.put(RESERVED, String.valueOf(reserveds.nextInt()));
				inventory.put(VIRTUAL_HOLD, String.valueOf(virtualHolds.nextInt()));
				commands.add(config.getInventory().getIndex(), docId, 1.0, inventory, addOptions);
			}

		});
		synchronized (requests) {
			requests.removeIf(r -> r.requester.equals(requester));
			this.requests.add(new GeneratorRequest(ZonedDateTime.now(), requester, storeSkus));
		}
	}

}
