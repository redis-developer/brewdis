package com.redislabs.demo.brewdis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.demo.brewdis.web.BrewerySuggestion;
import com.redislabs.demo.brewdis.web.Category;
import com.redislabs.demo.brewdis.web.Style;
import com.redislabs.mesclun.RedisModulesAsyncCommands;
import com.redislabs.mesclun.RedisModulesCommands;
import com.redislabs.mesclun.StatefulRedisModulesConnection;
import com.redislabs.mesclun.search.AggregateOptions;
import com.redislabs.mesclun.search.AggregateResults;
import com.redislabs.mesclun.search.CreateOptions;
import com.redislabs.mesclun.search.Field;
import com.redislabs.mesclun.search.IndexInfo;
import com.redislabs.mesclun.search.Order;
import com.redislabs.mesclun.search.RediSearchCommands;
import com.redislabs.mesclun.search.RediSearchUtils;
import com.redislabs.mesclun.search.SugaddOptions;
import com.redislabs.mesclun.search.aggregate.GroupBy;
import com.redislabs.mesclun.search.aggregate.Limit;
import com.redislabs.mesclun.search.aggregate.SortBy;
import com.redislabs.mesclun.search.aggregate.reducers.CountDistinct;
import com.redislabs.riot.ProcessorOptions;
import com.redislabs.riot.RedisOptions;
import com.redislabs.riot.file.FileImportCommand;
import com.redislabs.riot.file.RiotFile;
import com.redislabs.riot.redis.HsetCommand;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.core.io.Resource;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.redislabs.demo.brewdis.BrewdisField.*;

@Component
@Slf4j
public class DataLoader implements InitializingBean {

    @Value("classpath:english_stopwords.txt")
    private Resource stopwordsResource;
    @Autowired
    private StatefulRedisModulesConnection<String, String> connection;
    @Autowired
    private GenericObjectPool<StatefulRedisModulesConnection<String, String>> pool;
    @Autowired
    private Config config;
    @Autowired
    private RedisProperties redisProperties;
    @Getter
    private List<Category> categories;
    @Getter
    private Map<String, List<Style>> styles = new HashMap<>();
    private List<String> stopwords;

    @Override
    public void afterPropertiesSet() throws Exception {
        this.stopwords = new BufferedReader(
                new InputStreamReader(stopwordsResource.getInputStream(), StandardCharsets.UTF_8)).lines()
                .collect(Collectors.toList());
    }

    public void execute() throws Exception {
        loadStores();
        loadProducts();
        loadBreweries();
        loadCategoriesAndStyles();
        loadFoodPairings();
    }

    @SuppressWarnings("unchecked")
    private void loadStores() throws Exception {
        RediSearchCommands<String, String> commands = connection.sync();
        String index = config.getStore().getIndex();
        try {
            IndexInfo<String, String> info = RediSearchUtils.getInfo(commands.indexInfo(index));
            if (info.getNumDocs() >= config.getStore().getCount()) {
                log.info("Found {} stores - skipping load", Math.round(info.getNumDocs()));
                return;
            }
            commands.dropIndex(index);
        } catch (RedisCommandExecutionException e) {
            if (!e.getMessage().equals("Unknown Index name")) {
                throw e;
            }
        }
        commands.create(index, CreateOptions.<String, String>builder().prefix(config.getStore().getKeyspace() + config.getKeySeparator()).build(), Field.tag(STORE_ID).sortable(true).build(), Field.text("description").build(),
                Field.tag("market").sortable(true).build(), Field.tag("parent").sortable(true).build(),
                Field.text("address").build(), Field.text("city").sortable(true).build(),
                Field.tag("country").sortable(true).build(), Field.tag("inventoryAvailableToSell").sortable(true).build(),
                Field.tag("isDefault").sortable(true).build(), Field.tag("preferred").sortable(true).build(),
                Field.numeric("latitude").sortable(true).build(), Field.geo(LOCATION).build(),
                Field.numeric("longitude").sortable(true).build(), Field.tag("rollupInventory").sortable(true).build(),
                Field.tag("state").sortable(true).build(), Field.tag("type").sortable(true).build(),
                Field.tag("postalCode").sortable(true).build());
        RiotFile file = new RiotFile();
        configure(file.getRedisOptions());
        FileImportCommand command = new FileImportCommand();
        command.setApp(file);
        command.setFiles(Collections.singletonList(config.getStore().getUrl()));
        command.getOptions().setHeader(true);
        ProcessorOptions processorOptions = new ProcessorOptions();
        SpelExpressionParser parser = new SpelExpressionParser();
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put(LOCATION, parser.parseExpression("#geo(longitude,latitude)"));
        processorOptions.setSpelFields(fields);
        command.setProcessorOptions(processorOptions);
        HsetCommand hset = new HsetCommand();
        hset.setKeyspace(config.getStore().getKeyspace());
        hset.setKeys(new String[]{STORE_ID});
        command.setRedisCommands(Collections.singletonList(hset));
        command.execute();
    }

    @SuppressWarnings("unchecked")
    private void loadProducts() throws Exception {
        RediSearchCommands<String, String> commands = connection.sync();
        String index = config.getProduct().getIndex();
        try {
            IndexInfo<String, String> info = RediSearchUtils.getInfo(commands.indexInfo(index));
            if (info.getNumDocs() >= config.getProduct().getLoad().getCount()) {
                log.info("Found {} products - skipping load", Math.round(info.getNumDocs()));
                return;
            }
            commands.dropIndex(index);
        } catch (RedisCommandExecutionException e) {
            if (!e.getMessage().equals("Unknown Index name")) {
                throw e;
            }
        }
        commands.create(index, CreateOptions.<String, String>builder().prefix(config.getProduct().getKeyspace() + config.getKeySeparator()).build(), Field.tag(PRODUCT_ID).sortable(true).build(),
                Field.text(PRODUCT_NAME).sortable(true).build(),
                Field.text(PRODUCT_DESCRIPTION).matcher(Field.Text.PhoneticMatcher.English).build(),
                Field.tag(PRODUCT_LABEL).build(),
                Field.tag(CATEGORY_ID).sortable(true).build(),
                Field.text(CATEGORY_NAME).build(),
                Field.tag(STYLE_ID).sortable(true).build(),
                Field.text(STYLE_NAME).build(),
                Field.tag(BREWERY_ID).sortable(true).build(),
                Field.text(BREWERY_NAME).build(),
                Field.text(FOOD_PAIRINGS).sortable(true).build(),
                Field.tag("isOrganic").sortable(true).build(),
                Field.numeric("abv").sortable(true).build(),
                Field.numeric("ibu").sortable(true).build());
        RiotFile file = new RiotFile();
        configure(file.getRedisOptions());
        FileImportCommand command = new FileImportCommand();
        command.setApp(file);
        command.setFiles(Collections.singletonList(config.getProduct().getUrl()));
        ProcessorOptions processorOptions = new ProcessorOptions();
        SpelExpressionParser parser = new SpelExpressionParser();
        Map<String, Expression> fields = new LinkedHashMap<>();
        fields.put(PRODUCT_ID, parser.parseExpression("id"));
        fields.put(PRODUCT_LABEL, parser.parseExpression("containsKey('labels')"));
        fields.put(CATEGORY_ID, parser.parseExpression("style.category.id"));
        fields.put(CATEGORY_NAME, parser.parseExpression("style.category.name"));
        fields.put(STYLE_NAME, parser.parseExpression("style.shortName"));
        fields.put(STYLE_ID, parser.parseExpression("style.id"));
        fields.put(BREWERY_ID, parser.parseExpression("containsKey('breweries')?breweries[0].id:null"));
        fields.put(BREWERY_NAME, parser.parseExpression("containsKey('breweries')?breweries[0].nameShortDisplay:null"));
        fields.put(BREWERY_ICON, parser.parseExpression("containsKey('breweries')?breweries[0].containsKey('images')?breweries[0].get('images').get('icon'):null:null"));
        processorOptions.setSpelFields(fields);
        command.setProcessorOptions(processorOptions);
        HsetCommand hset = new HsetCommand();
        hset.setKeyspace(config.getProduct().getKeyspace());
        hset.setKeys(new String[]{PRODUCT_ID});
        command.setRedisCommands(Collections.singletonList(hset));
        command.execute();
    }

    private void configure(RedisOptions redisOptions) {
        redisOptions.setHost(redisProperties.getHost());
        redisOptions.setPort(redisProperties.getPort());
        if (redisProperties.getClientName() != null) {
            redisOptions.setClientName(redisProperties.getClientName());
        }
        redisOptions.setDatabase(redisProperties.getDatabase());
        if (redisProperties.getPassword() != null) {
            redisOptions.setPassword(redisProperties.getPassword().toCharArray());
        }
        redisOptions.setTls(redisProperties.isSsl());
    }

    private void loadCategoriesAndStyles() {
        log.info("Loading categories");
        RediSearchCommands<String, String> commands = connection.sync();
        String index = config.getProduct().getIndex();
        AggregateResults<String> results = commands.aggregate(index, "*",
                AggregateOptions.builder().load(CATEGORY_NAME)
                        .operation(GroupBy.properties(CATEGORY_ID, CATEGORY_NAME)
                                .reducer(CountDistinct.property(PRODUCT_ID).as(COUNT).build()).build())
                        .build());
        this.categories = results.stream()
                .map(r -> Category.builder().id((String) r.get(CATEGORY_ID)).name((String) r.get(CATEGORY_NAME)).build())
                .sorted(Comparator.comparing(Category::getName, Comparator.nullsLast(Comparator.naturalOrder())))
                .collect(Collectors.toList());
        log.info("Loading styles");
        this.categories.forEach(category -> {
            AggregateResults<String> styleResults = commands.aggregate(index,
                    config.tag(CATEGORY_ID, category.getId()),
                    AggregateOptions.builder().load(STYLE_NAME)
                            .operation(GroupBy.properties(STYLE_ID, STYLE_NAME)
                                    .reducer(CountDistinct.property(PRODUCT_ID).as(COUNT).build()).build())
                            .build());
            List<Style> styleList = styleResults.stream()
                    .map(r -> Style.builder().id((String) r.get(STYLE_ID)).name((String) r.get(STYLE_NAME)).build())
                    .sorted(Comparator.comparing(Style::getName, Comparator.nullsLast(Comparator.naturalOrder())))
                    .collect(Collectors.toList());
            this.styles.put(category.getId(), styleList);
        });
    }

    private void loadBreweries() {
        RediSearchCommands<String, String> commands = connection.sync();
        try {
            Long length = commands.suglen(config.getProduct().getBrewery().getIndex());
            if (length != null && length > 0) {
                log.info("Found {} breweries - skipping load", length);
                return;
            }
        } catch (RedisCommandExecutionException e) {
            // ignore
        }
        log.info("Loading breweries");
        AggregateResults<String> results = commands.aggregate(config.getProduct().getIndex(), "*",
                AggregateOptions.builder().load(BREWERY_NAME).load(BREWERY_ICON)
                        .operation(GroupBy.properties(BREWERY_ID, BREWERY_NAME, BREWERY_ICON)
                                .reducer(CountDistinct.property(PRODUCT_ID).as(COUNT).build()).build())
                        .build());
        ObjectMapper mapper = new ObjectMapper();
        results.forEach(r -> {
            BrewerySuggestion.Payload payloadObject = new BrewerySuggestion.Payload();
            payloadObject.setId((String) r.get(BREWERY_ID));
            payloadObject.setIcon((String) r.get(BREWERY_ICON));
            String payload = null;
            try {
                payload = mapper.writeValueAsString(payloadObject);
            } catch (JsonProcessingException e) {
                log.error("Could not serialize brewery payload {}", payloadObject, e);
            }
            String breweryName = (String) r.get(BREWERY_NAME);
            if (breweryName == null) {
                return;
            }
            double count = Double.parseDouble((String) r.get(COUNT));
            commands.sugadd(config.getProduct().getBrewery().getIndex(), breweryName, count, SugaddOptions.<String>builder().payload(payload).build());
        });
        log.info("Loaded {} breweries", results.size());
    }

    private void loadFoodPairings() throws Exception {
        RedisModulesCommands<String, String> sync = connection.sync();
        sync.del(config.getProduct().getFoodPairings().getIndex());
        log.info("Loading food pairings");
        String index = config.getProduct().getIndex();
        AggregateResults<String> results = sync.aggregate(index, "*", AggregateOptions.builder()
                .operation(GroupBy.property(FOOD_PAIRINGS).reducer(CountDistinct.property(PRODUCT_ID).as(COUNT).build()).build())
                .operation(SortBy.property(SortBy.Property.name(COUNT).order(Order.DESC)).build())
                .operation(Limit.offset(0).num(config.getProduct().getFoodPairings().getLimit())).build());
        try (StatefulRedisModulesConnection<String, String> connection = pool.borrowObject()) {
            RedisModulesAsyncCommands<String, String> async = connection.async();
            async.setAutoFlushCommands(false);
            List<RedisFuture<?>> futures = new ArrayList<>();
            results.forEach(r -> {
                String foodPairings = (String) r.get(FOOD_PAIRINGS);
                if (foodPairings == null || foodPairings.trim().isEmpty()) {
                    return;
                }
                Arrays.stream(foodPairings.split("[,\\n]")).map(this::clean).filter(s -> s.split(" ").length <= 2)
                        .forEach(food -> futures.add(async.sugadd(config.getProduct().getFoodPairings().getIndex(), food, 1.0, SugaddOptions.<String>builder().increment(true).build())));
            });
            async.flushCommands();
            LettuceFutures.awaitAll(connection.getTimeout(), futures.toArray(new RedisFuture[0]));
            async.setAutoFlushCommands(true);
            log.info("Loaded {} food pairings", results.size());
        }
    }

    private String clean(String food) {
        List<String> allWords = Stream.of(food.toLowerCase().split(" "))
                .collect(Collectors.toCollection(ArrayList<String>::new));
        allWords.removeAll(stopwords);
        String result = String.join(" ", allWords).trim();
        if (result.endsWith(".")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

}
