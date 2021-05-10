package com.redislabs.demo.brewdis;

import com.redislabs.demo.brewdis.Config.InventoryGeneratorConfig;
import com.redislabs.mesclun.RedisModulesCommands;
import com.redislabs.mesclun.StatefulRedisModulesConnection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.PrimitiveIterator.OfInt;

import static com.redislabs.demo.brewdis.BrewdisField.*;

@Component
@Slf4j
public class InventoryGenerator implements InitializingBean {

    @Data
    @AllArgsConstructor
    public class GeneratorRequest {
        private ZonedDateTime time;
        private String requester;
        private List<StoreSku> storeSkus;

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
    private Config config;
    @Autowired
    private StringRedisTemplate redis;
    @Autowired
    private StatefulRedisModulesConnection<String, String> connection;
    private Random random = new Random();
    private OfInt deltas;
    private final List<GeneratorRequest> requests = new ArrayList<>();
    private OfInt onHands;
    private OfInt allocateds;
    private OfInt reserveds;
    private OfInt virtualHolds;

    @Override
    public void afterPropertiesSet() {
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
            requests.removeIf(GeneratorRequest::isExpired);
        }
    }

    public void add(String requester, List<String> stores, List<String> skus) {
        if (stores.isEmpty()) {
            return;
        }
        if (skus.isEmpty()) {
            return;
        }
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
            RedisModulesCommands<String, String> commands = connection.sync();
            String docId = config.concat(config.getInventory().getKeyspace(), store, sku);
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
                inventory.put(ON_HAND, String.valueOf(onHands.nextInt()));
                inventory.put(ALLOCATED, String.valueOf(allocateds.nextInt()));
                inventory.put(RESERVED, String.valueOf(reserveds.nextInt()));
                inventory.put(VIRTUAL_HOLD, String.valueOf(virtualHolds.nextInt()));
                commands.hset(docId, inventory);
            }

        });
        synchronized (requests) {
            requests.removeIf(r -> r.requester.equals(requester));
            this.requests.add(new GeneratorRequest(ZonedDateTime.now(), requester, storeSkus));
        }
    }

}
