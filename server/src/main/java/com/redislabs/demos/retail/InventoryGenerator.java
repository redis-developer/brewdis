package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.*;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
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
import org.springframework.data.redis.stream.Subscription;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.AddOptions;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryGenerator implements InitializingBean, DisposableBean {

	@Autowired
	private RetailConfig config;
	@Autowired
	private RedisTemplate<String, String> template;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
	@Autowired
	private SimpMessageSendingOperations sendingOps;
	private Subscription subscription;

	@Override
	public void afterPropertiesSet() throws Exception {
		executor.scheduleAtFixedRate(new InventoryCleanup(), config.getInventory().getGenerator().getCleanupPeriod(),
				config.getInventory().getGenerator().getCleanupPeriod(), TimeUnit.SECONDS);
	}

	@Override
	public void destroy() throws Exception {
		log.info("Cancelling inventory subscription");
		subscription.cancel();
		executor.shutdownNow();
	}

	public void generate(List<String> skus, List<String> stores) {
		if (stores.isEmpty()) {
			return;
		}
		if (skus.isEmpty()) {
			return;
		}
		log.info("Generating inventory for stores {} and skus {}", stores, skus);
		ScheduledFuture<?> future = executor.scheduleAtFixedRate(new InventoryUpdate(skus, stores), 0,
				config.getInventory().getGenerator().getSleep(), TimeUnit.MILLISECONDS);
		executor.schedule(new Runnable() {
			@Override
			public void run() {
				future.cancel(true);
			}
		}, 10, TimeUnit.MINUTES);
	}

	private class InventoryCleanup implements Runnable {

		@Override
		public void run() {
			ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC)
					.minus(Duration.ofSeconds(config.getInventory().getGenerator().getCleanupPeriod()));
			String query = "@epoch:[0 " + time.toEpochSecond() + "]";
			SearchResults<String, String> results = connection.sync().search(config.getInventory().getIndex(), query,
					SearchOptions.builder().noContent(true)
							.limit(Limit.builder().num(config.getInventory().getGenerator().getCleanupLimit()).build())
							.build());
			results.forEach(r -> connection.sync().del(config.getInventory().getIndex(), r.getDocumentId(), true));
			if (results.size() > 0) {
				log.info("Deleted {} inventory entries", results.size());
			}
		}
	}

	private class InventoryUpdate implements Runnable {

		private AddOptions addOptions = AddOptions.builder().replace(true).replacePartial(true).build();
		private Random random = new Random();
		private OfInt onHandDeltas;
		private List<String> skus;
		private List<String> stores;
		private OfInt restockingDelays;
		private OfInt onHands;
		private OfInt allocateds;
		private OfInt reserveds;
		private OfInt virtualHolds;

		public InventoryUpdate(List<String> skus, List<String> stores) {
			this.skus = skus;
			this.stores = stores;
			this.onHandDeltas = random.ints(config.getInventory().getGenerator().getDeltaMin(),
					config.getInventory().getGenerator().getDeltaMax()).iterator();
			this.restockingDelays = random.ints(config.getInventory().getGenerator().getRestockingDelayMin(),
					config.getInventory().getGenerator().getRestockingDelayMax()).iterator();
			this.onHands = random.ints(config.getInventory().getGenerator().getOnHandMin(),
					config.getInventory().getGenerator().getOnHandMax()).iterator();
			this.allocateds = random.ints(config.getInventory().getGenerator().getAllocatedMin(),
					config.getInventory().getGenerator().getAllocatedMax()).iterator();
			this.reserveds = random.ints(config.getInventory().getGenerator().getReservedMin(),
					config.getInventory().getGenerator().getReservedMax()).iterator();
			this.virtualHolds = random.ints(config.getInventory().getGenerator().getVirtualHoldMin(),
					config.getInventory().getGenerator().getVirtualHoldMax()).iterator();
		}

		@Override
		public void run() {
			String store = stores.get(random.nextInt(stores.size()));
			String sku = skus.get(random.nextInt(skus.size()));
			int delta = onHandDeltas.nextInt();
			update(store, sku, delta);
		}

		private void update(String store, String sku, int onHandDelta) {
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
			int previousOnHand = Integer.parseInt(inventory.get(ON_HAND));
			int newOnHand = previousOnHand + onHandDelta;
			if (newOnHand < 0) {
				newOnHand = 0;
			}
			if (newOnHand == 0) {
				executor.schedule(new Runnable() {

					@Override
					public void run() {
						update(store, sku, onHands.nextInt());
					}
				}, restockingDelays.nextInt(), TimeUnit.SECONDS);
			}
			if (newOnHand == previousOnHand) {
				return;
			}
			inventory.put(ON_HAND, String.valueOf(newOnHand));
			int allocated = Integer.parseInt(inventory.get(ALLOCATED));
			int reserved = Integer.parseInt(inventory.get(RESERVED));
			int virtualHold = Integer.parseInt(inventory.get(VIRTUAL_HOLD));
			int availableToPromise = newOnHand - (allocated + reserved + virtualHold);
			if (availableToPromise < 0) {
				log.info("Skipping because availableToPromise<0: {}", inventory);
				return;
			}
			inventory.put(AVAILABLE_TO_PROMISE, String.valueOf(availableToPromise));
			ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC);
			inventory.put(TIME, time.format(DateTimeFormatter.ISO_INSTANT));
			inventory.put(EPOCH, String.valueOf(time.toEpochSecond()));
			inventory.put(DELTA, String.valueOf(onHandDelta));
			template.opsForStream().add(config.getInventory().getStream(), inventory);
			sendingOps.convertAndSend(config.getStomp().getInventoryTopic(), inventory);
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

}
