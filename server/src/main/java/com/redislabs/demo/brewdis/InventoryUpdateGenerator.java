package com.redislabs.demo.brewdis;

import static com.redislabs.demo.brewdis.Field.ALLOCATED;
import static com.redislabs.demo.brewdis.Field.ON_HAND;
import static com.redislabs.demo.brewdis.Field.PRODUCT_ID;
import static com.redislabs.demo.brewdis.Field.RESERVED;
import static com.redislabs.demo.brewdis.Field.STORE_ID;
import static com.redislabs.demo.brewdis.Field.VIRTUAL_HOLD;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator.OfInt;
import java.util.Random;
import java.util.Set;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.redislabs.demo.brewdis.BrewdisConfig.InventoryGeneratorConfig;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryUpdateGenerator implements InitializingBean {

	@Autowired
	private BrewdisConfig config;
	@Autowired
	private RedisTemplate<String, String> template;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	private Random random = new Random();
	private OfInt allocated;
	private Set<String> stores = new HashSet<>();
	private Set<String> skus = new HashSet<>();
	private OfInt onHands;
	private OfInt allocateds;
	private OfInt reserveds;
	private OfInt virtualHolds;
	private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();

	@Override
	public void afterPropertiesSet() throws Exception {
		InventoryGeneratorConfig generatorConfig = config.getInventory().getGenerator();
		this.allocated = random.ints(generatorConfig.getDeltaMin(), generatorConfig.getDeltaMax()).iterator();
		this.onHands = random.ints(generatorConfig.getOnHandMin(), generatorConfig.getOnHandMax()).iterator();
		this.allocateds = random.ints(generatorConfig.getAllocatedMin(), generatorConfig.getAllocatedMax()).iterator();
		this.reserveds = random.ints(generatorConfig.getReservedMin(), generatorConfig.getReservedMax()).iterator();
		this.virtualHolds = random.ints(generatorConfig.getVirtualHoldMin(), generatorConfig.getVirtualHoldMax())
				.iterator();

	}

	@Scheduled(fixedRateString = "${inventory.generator.rate}")
	public void generate() {
		if (stores.isEmpty()) {
			return;
		}
		if (skus.isEmpty()) {
			return;
		}
		String stream = config.getInventory().getInputStream();
		String store = stores.toArray(new String[stores.size()])[random.nextInt(stores.size())];
		String sku = skus.toArray(new String[skus.size()])[random.nextInt(skus.size())];
		Map<String, String> fields = Map.of(STORE_ID, store, PRODUCT_ID, sku, ALLOCATED,
				String.valueOf(allocated.nextInt()));
		log.debug("XADD stream {} fields {}");
		template.opsForStream().add(stream, fields);
	}

	public void add(List<String> stores, String sku) {
		log.info("Adding stores {} and sku {}", stores, sku);
		for (String store : stores) {
			String productDocId = config.concat(config.getProduct().getKeyspace(), sku);
			Map<String, String> productDoc = connection.sync().get(config.getProduct().getIndex(), productDocId);
			if (productDoc == null) {
				log.warn("Unknown product {}", productDocId);
			}
			String storeDocId = config.concat(config.getStore().getKeyspace(), store);
			Map<String, String> storeDoc = connection.sync().get(config.getStore().getIndex(), storeDocId);
			if (storeDoc == null) {
				log.warn("Unknown store {}", storeDocId);
			}
			Map<String, String> inventory = new HashMap<>();
			config.getProduct().getInventoryMapping().forEach((k, v) -> inventory.put(v, productDoc.get(k)));
			config.getStore().getInventoryMapping().forEach((k, v) -> inventory.put(v, storeDoc.get(k)));
			inventory.put(ON_HAND, String.valueOf(onHands.nextInt()));
			inventory.put(ALLOCATED, String.valueOf(allocateds.nextInt()));
			inventory.put(RESERVED, String.valueOf(reserveds.nextInt()));
			inventory.put(VIRTUAL_HOLD, String.valueOf(virtualHolds.nextInt()));
			String docId = config.concat(config.getInventory().getKeyspace(), store, sku);
			connection.sync().add(config.getInventory().getIndex(), docId, 1.0, inventory, addOptions);
		}
		this.stores.addAll(stores);
		this.skus.add(sku);

	}

}
