package com.redislabs.demo.brewdis;

import com.redislabs.mesclun.RedisModulesCommands;
import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.search.*;
import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.Random;

import static com.redislabs.demo.brewdis.BrewdisField.*;

@Component
@Slf4j
public class InventoryManager implements InitializingBean, DisposableBean, StreamListener<String, MapRecord<String, String, String>> {

    @Autowired
    private Config config;
    @Autowired
    private StatefulRedisModulesConnection<String, String> connection;
    @Autowired
    private StringRedisTemplate redis;
    private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
    private Subscription subscription;
    private PrimitiveIterator.OfInt onHand;
    private PrimitiveIterator.OfInt allocated;
    private PrimitiveIterator.OfInt reserved;
    private PrimitiveIterator.OfInt virtualHold;

    @SuppressWarnings("unchecked")
    @Override
    public void afterPropertiesSet() throws Exception {
        Random random = new Random();
        Config.InventoryGeneratorConfig generator = config.getInventory().getGenerator();
        this.onHand = random.ints(generator.getOnHandMin(), generator.getOnHandMax()).iterator();
        this.allocated = random.ints(generator.getAllocatedMin(), generator.getAllocatedMax()).iterator();
        this.reserved = random.ints(generator.getReservedMin(), generator.getReservedMax()).iterator();
        this.virtualHold = random.ints(generator.getVirtualHoldMin(), generator.getVirtualHoldMax()).iterator();
        RedisModulesCommands<String, String> commands = connection.sync();
        String index = config.getInventory().getIndex();
        log.info("Dropping {} index", index);
        try {
            commands.dropIndex(index);
        } catch (RedisCommandExecutionException e) {
            if (!e.getMessage().equals("Unknown Index name")) {
                throw e;
            }
        }
        log.info("Creating {} index", index);
        commands.create(index, CreateOptions.<String, String>builder().prefix(config.getInventory().getKeyspace() + config.getKeySeparator()).build(), Field.tag(STORE_ID).sortable(true).build(),
                Field.tag(PRODUCT_ID).sortable(true).build(),
                Field.geo(LOCATION).build(),
                Field.numeric(AVAILABLE_TO_PROMISE).sortable(true).build(),
                Field.numeric(ON_HAND).sortable(true).build(),
                Field.numeric(ALLOCATED).sortable(true).build(),
                Field.numeric(RESERVED).sortable(true).build(),
                Field.numeric(VIRTUAL_HOLD).sortable(true).build(),
                Field.numeric(EPOCH).sortable(true).build());
        commands.del(config.getInventory().getUpdateStream());
        this.container = StreamMessageListenerContainer.create(redis.getConnectionFactory(), StreamMessageListenerContainerOptions.builder().pollTimeout(Duration.ofMillis(config.getStreamPollTimeout())).build());
        container.start();
        this.subscription = container.receive(StreamOffset.fromStart(config.getInventory().getUpdateStream()), this);
        subscription.await(Duration.ofSeconds(2));
    }

    @Override
    public void destroy() {
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
        String id = config.concat(store, sku);
        String docId = config.concat(config.getInventory().getKeyspace(), id);
        RedisModulesCommands<String, String> commands = connection.sync();
        if (commands.hgetall(docId).isEmpty()) {
            Map<String, String> productDoc = commands.hgetall(config.concat(config.getProduct().getKeyspace(), sku));
            if (productDoc.isEmpty()) {
                log.warn("Unknown product '{}'", sku);
                return;
            }
            Map<String, String> storeDoc = commands.hgetall(config.concat(config.getStore().getKeyspace(), store));
            if (storeDoc.isEmpty()) {
                log.warn("Unknown store '{}'", store);
                return;
            }
            Map<String, String> inventory = new HashMap<>();
            config.getProduct().getInventoryMapping().forEach((k, v) -> inventory.put(v, productDoc.get(k)));
            config.getStore().getInventoryMapping().forEach((k, v) -> inventory.put(v, storeDoc.get(k)));
            inventory.put(ON_HAND, String.valueOf(onHand.nextInt()));
            inventory.put(ALLOCATED, String.valueOf(allocated.nextInt()));
            inventory.put(RESERVED, String.valueOf(reserved.nextInt()));
            inventory.put(VIRTUAL_HOLD, String.valueOf(virtualHold.nextInt()));
            commands.hset(docId, inventory);
        }
        Map<String, String> inventory = commands.hgetall(docId);
        inventory.put(STORE_ID, store);
        inventory.put(PRODUCT_ID, sku);
        if (message.getValue().containsKey(ON_HAND)) {
            int delta = getInt(message.getValue(), ON_HAND);
            log.info("Received restocking for {}:{} {}={}", store, sku, DELTA, delta);
            int previousOnHand = getInt(inventory, ON_HAND);
            int onHand = previousOnHand + delta;
            inventory.put(ON_HAND, String.valueOf(onHand));
        }
        if (message.getValue().containsKey(ALLOCATED)) {
            int delta = getInt(message.getValue(), ALLOCATED);
            int previousAllocated = getInt(inventory, ALLOCATED);
            int allocated = previousAllocated + delta;
            inventory.put(ALLOCATED, String.valueOf(allocated));
        }
        int availableToPromise = availableToPromise(inventory);
        if (availableToPromise < 0) {
            return;
        }
        int delta = 0;
        if (inventory.containsKey(AVAILABLE_TO_PROMISE)) {
            int previousAvailableToPromise = getInt(inventory, AVAILABLE_TO_PROMISE);
            delta = availableToPromise - previousAvailableToPromise;
        }
        inventory.put(DELTA, String.valueOf(delta));
        inventory.put(AVAILABLE_TO_PROMISE, String.valueOf(availableToPromise));
        ZonedDateTime time = ZonedDateTime.now();
        inventory.put(TIME, time.format(DateTimeFormatter.ISO_INSTANT));
        inventory.put(EPOCH, String.valueOf(time.toEpochSecond()));
        inventory.put(LEVEL, config.getInventory().level(availableToPromise));
        redis.opsForStream().add(config.getInventory().getStream(), inventory);
        try {
            commands.hset(docId, inventory);
        } catch (RedisCommandExecutionException e) {
            log.error("Could not add document {}: {}", docId, inventory, e);
        }
    }

    private int availableToPromise(Map<String, String> inventory) {
        int allocated = getInt(inventory, ALLOCATED);
        int reserved = getInt(inventory, RESERVED);
        int virtualHold = getInt(inventory, VIRTUAL_HOLD);
        int demand = allocated + reserved + virtualHold;
        int supply = getInt(inventory, ON_HAND);
        return supply - demand;
    }

    private int getInt(Map<String, String> map, String field) {
        return Integer.parseInt(map.getOrDefault(field, "0"));
    }

    @Scheduled(fixedRateString = "${inventory.cleanup.rate}")
    public void cleanup() {
        ZonedDateTime time = ZonedDateTime.now().minus(Duration.ofSeconds(config.getInventory().getCleanup().getAgeThreshold()));
        String query = "@" + EPOCH + ":[0 " + time.toEpochSecond() + "]";
        String index = config.getInventory().getIndex();
        RedisModulesCommands<String, String> commands = connection.sync();
        SearchResults<String, String> results = commands.search(index, query, SearchOptions.builder().noContent(true)
                .limit(SearchOptions.Limit.offset(0).num(config.getInventory().getCleanup().getSearchLimit())).build());
        if (!results.isEmpty()) {
            log.info("Deleting {} docs", results.size());
            commands.del(results.stream().map(Document::getId).toArray(String[]::new));
        }
        redis.opsForStream().trim(config.getInventory().getUpdateStream(),
                config.getInventory().getCleanup().getStreamTrimCount());
        redis.opsForStream().trim(config.getInventory().getStream(),
                config.getInventory().getCleanup().getStreamTrimCount());
    }

}
