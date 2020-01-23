package com.redislabs.demo.brewdis;

import static com.redislabs.demo.brewdis.BrewdisField.BREWERY_ICON;
import static com.redislabs.demo.brewdis.BrewdisField.BREWERY_ID;
import static com.redislabs.demo.brewdis.BrewdisField.BREWERY_NAME;
import static com.redislabs.demo.brewdis.BrewdisField.CATEGORY_ID;
import static com.redislabs.demo.brewdis.BrewdisField.CATEGORY_NAME;
import static com.redislabs.demo.brewdis.BrewdisField.COUNT;
import static com.redislabs.demo.brewdis.BrewdisField.FOOD_PAIRINGS;
import static com.redislabs.demo.brewdis.BrewdisField.LOCATION;
import static com.redislabs.demo.brewdis.BrewdisField.PRODUCT_DESCRIPTION;
import static com.redislabs.demo.brewdis.BrewdisField.PRODUCT_ID;
import static com.redislabs.demo.brewdis.BrewdisField.PRODUCT_LABEL;
import static com.redislabs.demo.brewdis.BrewdisField.PRODUCT_NAME;
import static com.redislabs.demo.brewdis.BrewdisField.STORE_ID;
import static com.redislabs.demo.brewdis.BrewdisField.STYLE_ID;
import static com.redislabs.demo.brewdis.BrewdisField.STYLE_NAME;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.demo.brewdis.WebController.BrewerySuggestionPayload;
import com.redislabs.demo.brewdis.WebController.Category;
import com.redislabs.demo.brewdis.WebController.Style;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.RediSearchUtils;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateResults;
import com.redislabs.lettusearch.aggregate.Group;
import com.redislabs.lettusearch.aggregate.Limit;
import com.redislabs.lettusearch.aggregate.Order;
import com.redislabs.lettusearch.aggregate.Sort;
import com.redislabs.lettusearch.aggregate.SortProperty;
import com.redislabs.lettusearch.aggregate.reducer.CountDistinct;
import com.redislabs.lettusearch.index.IndexInfo;
import com.redislabs.lettusearch.search.DropOptions;
import com.redislabs.lettusearch.search.Schema;
import com.redislabs.lettusearch.search.field.Field;
import com.redislabs.lettusearch.search.field.GeoField;
import com.redislabs.lettusearch.search.field.NumericField;
import com.redislabs.lettusearch.search.field.PhoneticMatcher;
import com.redislabs.lettusearch.search.field.TagField;
import com.redislabs.lettusearch.search.field.TextField;
import com.redislabs.picocliredis.RedisOptions;
import com.redislabs.picocliredis.Server;
import com.redislabs.riot.cli.ProcessorOptions;
import com.redislabs.riot.cli.file.FileImportCommand;
import com.redislabs.riot.cli.file.FileReaderOptions;
import com.redislabs.riot.cli.file.ResourceOptions;
import com.redislabs.riot.redis.writer.KeyBuilder;
import com.redislabs.riot.redis.writer.map.FtAdd;

import io.lettuce.core.RedisCommandExecutionException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class DataLoader implements InitializingBean {

	@Value("classpath:english_stopwords.txt")
	private Resource stopwordsResource;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private Config config;
	@Autowired
	private RedisProperties redisProperties;
	@Getter
	private List<Category> categories;
	@Getter
	private Map<String, List<Style>> styles = new HashMap<>();
	private List<String> stopwords;
	private RedisOptions connectionOptions = new RedisOptions();

	@Override
	public void afterPropertiesSet() throws Exception {
		this.stopwords = new BufferedReader(
				new InputStreamReader(stopwordsResource.getInputStream(), StandardCharsets.UTF_8)).lines()
						.collect(Collectors.toList());
		this.connectionOptions.servers(new Server(redisProperties.getHost(), redisProperties.getPort()));
	}

	public void execute() throws IOException, URISyntaxException {
		loadStores();
		loadProducts();
		loadBreweries();
		loadCategoriesAndStyles();
		loadFoodPairings();
	}

	private void loadStores() {
		RediSearchCommands<String, String> commands = connection.sync();
		String index = config.getStore().getIndex();
		try {
			IndexInfo info = RediSearchUtils.getInfo(commands.indexInfo(index));
			if (info.numDocs() >= config.getStore().getCount()) {
				log.info("Found {} stores - skipping load", info.numDocs());
				return;
			}
			commands.drop(index, DropOptions.builder().build());
		} catch (RedisCommandExecutionException e) {
			if (!e.getMessage().equals("Unknown Index name")) {
				throw e;
			}
		}
		Schema schema = Schema.builder().field(TagField.builder().name(STORE_ID).sortable(true).build())
				.field(Field.text("description")).field(Field.tag("market").sortable(true))
				.field(Field.tag("parent").sortable(true)).field(Field.text("address"))
				.field(Field.text("city").sortable(true))
				.field(TagField.builder().name("country").sortable(true).build())
				.field(TagField.builder().name("inventoryAvailableToSell").sortable(true).build())
				.field(TagField.builder().name("isDefault").sortable(true).build())
				.field(TagField.builder().name("preferred").sortable(true).build())
				.field(NumericField.builder().name("latitude").sortable(true).build())
				.field(GeoField.builder().name(LOCATION).build())
				.field(NumericField.builder().name("longitude").sortable(true).build())
				.field(TagField.builder().name("rollupInventory").sortable(true).build())
				.field(TagField.builder().name("state").sortable(true).build())
				.field(TagField.builder().name("type").sortable(true).build())
				.field(TagField.builder().name("postalCode").sortable(true).build()).build();
		commands.create(index, schema);
		FileImportCommand command = new FileImportCommand();
		FileReaderOptions readerOptions = new FileReaderOptions();
		readerOptions.resourceOptions(new ResourceOptions().url(URI.create(config.getStore().getUrl())));
		readerOptions.header(true);
		command.options(readerOptions);
		ProcessorOptions processorOptions = new ProcessorOptions();
		processorOptions.fields().put(LOCATION, "#geo(longitude,latitude)");
		command.processorOptions(processorOptions);
		FtAdd add = new FtAdd();
		add.index(index);
		add.keyBuilder(KeyBuilder.builder().prefix("store").fields(new String[] { STORE_ID }).build());
		command.execute(add);
	}

	private void loadProducts() throws URISyntaxException {
		RediSearchCommands<String, String> commands = connection.sync();
		String index = config.getProduct().getIndex();
		try {
			IndexInfo info = RediSearchUtils.getInfo(commands.indexInfo(index));
			if (info.numDocs() >= config.getProduct().getLoad().getCount()) {
				log.info("Found {} products - skipping load", info.numDocs());
				return;
			}
			commands.drop(index, DropOptions.builder().build());
		} catch (RedisCommandExecutionException e) {
			if (!e.getMessage().equals("Unknown Index name")) {
				throw e;
			}
		}
		Schema schema = Schema.builder().field(TagField.builder().name(PRODUCT_ID).sortable(true).build())
				.field(TextField.builder().name(PRODUCT_NAME).sortable(true).build())
				.field(TextField.builder().name(PRODUCT_DESCRIPTION).matcher(PhoneticMatcher.English).build())
				.field(TagField.builder().name(PRODUCT_LABEL).build())
				.field(TagField.builder().name(CATEGORY_ID).sortable(true).build())
				.field(TextField.builder().name(CATEGORY_NAME).build())
				.field(TagField.builder().name(STYLE_ID).sortable(true).build())
				.field(TextField.builder().name(STYLE_NAME).build())
				.field(TagField.builder().name(BREWERY_ID).sortable(true).build())
				.field(TextField.builder().name(BREWERY_NAME).build())
				.field(TextField.builder().name(FOOD_PAIRINGS).sortable(true).build())
				.field(TagField.builder().name("isOrganic").sortable(true).build())
				.field(NumericField.builder().name("abv").sortable(true).build())
				.field(NumericField.builder().name("ibu").sortable(true).build()).build();
		commands.create(index, schema);
		FileImportCommand command = new FileImportCommand();
		if (config.getProduct().getLoad().getSleep() != null) {
			command.sleep(config.getProduct().getLoad().getSleep());
		}
		FileReaderOptions readerOptions = new FileReaderOptions();
		readerOptions.resourceOptions(new ResourceOptions().url(new URI(config.getProduct().getUrl())));
		command.options(readerOptions);
		ProcessorOptions processorOptions = new ProcessorOptions();
		processorOptions.addField(PRODUCT_ID, "id");
		processorOptions.addField(PRODUCT_LABEL, "containsKey('labels')");
		processorOptions.addField(CATEGORY_ID, "style.category.id");
		processorOptions.addField(CATEGORY_NAME, "style.category.name");
		processorOptions.addField(STYLE_NAME, "style.shortName");
		processorOptions.addField(STYLE_ID, "style.id");
		processorOptions.addField(BREWERY_ID, "containsKey('breweries')?breweries[0].id:null");
		processorOptions.addField(BREWERY_NAME, "containsKey('breweries')?breweries[0].nameShortDisplay:null");
		processorOptions.addField(BREWERY_ICON,
				"containsKey('breweries')?breweries[0].containsKey('images')?breweries[0].get('images').get('icon'):null:null");
		command.processorOptions(processorOptions);
		FtAdd ftAdd = new FtAdd();
		ftAdd.index(index);
		ftAdd.keyBuilder(KeyBuilder.builder().prefix("product").field(PRODUCT_ID).build());
		command.execute(ftAdd);
	}

	private void loadCategoriesAndStyles() {
		log.info("Loading categories");
		RediSearchCommands<String, String> commands = connection.sync();
		String index = config.getProduct().getIndex();
		AggregateResults<String, String> results = commands.aggregate(index, "*",
				AggregateOptions.builder().load(CATEGORY_NAME)
						.operation(Group.builder().property(CATEGORY_ID).property(CATEGORY_NAME)
								.reducer(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
						.build());
		this.categories = results.stream()
				.map(r -> Category.builder().id(r.get(CATEGORY_ID)).name(r.get(CATEGORY_NAME)).build())
				.sorted(Comparator.comparing(Category::getName, Comparator.nullsLast(Comparator.naturalOrder())))
				.collect(Collectors.toList());
		log.info("Loading styles");
		this.categories.forEach(category -> {
			AggregateResults<String, String> styleResults = commands.aggregate(index,
					config.tag(CATEGORY_ID, category.getId()),
					AggregateOptions.builder().load(STYLE_NAME)
							.operation(Group.builder().property(STYLE_ID).property(STYLE_NAME)
									.reducer(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
							.build());
			List<Style> styleList = styleResults.stream()
					.map(r -> Style.builder().id(r.get(STYLE_ID)).name(r.get(STYLE_NAME)).build())
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
		AggregateResults<String, String> results = commands.aggregate(config.getProduct().getIndex(), "*",
				AggregateOptions.builder().load(BREWERY_NAME).load(BREWERY_ICON)
						.operation(Group.builder().property(BREWERY_ID).property(BREWERY_NAME).property(BREWERY_ICON)
								.reducer(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
						.build());
		ObjectMapper mapper = new ObjectMapper();
		results.forEach(r -> {
			BrewerySuggestionPayload payloadObject = new BrewerySuggestionPayload();
			payloadObject.setId(r.get(BREWERY_ID));
			payloadObject.setIcon(r.get(BREWERY_ICON));
			String payload = null;
			try {
				payload = mapper.writeValueAsString(payloadObject);
			} catch (JsonProcessingException e) {
				log.error("Could not serialize brewery payload {}", payloadObject, e);
			}
			String breweryName = r.get(BREWERY_NAME);
			if (breweryName == null) {
				return;
			}
			double count = Double.parseDouble(r.get(COUNT));
			commands.sugadd(config.getProduct().getBrewery().getIndex(), breweryName, count, payload);
		});
		log.info("Loaded {} breweries", results.size());
	}

	private void loadFoodPairings() throws IOException, URISyntaxException {
		RediSearchCommands<String, String> commands = connection.sync();
		commands.del(config.getProduct().getFoodPairings().getIndex());
		log.info("Loading food pairings");
		String index = config.getProduct().getIndex();
		AggregateResults<String, String> results = commands.aggregate(index, "*", AggregateOptions.builder()
				.operation(Group.builder().property(FOOD_PAIRINGS)
						.reducer(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
				.operation(Sort.builder().property(SortProperty.builder().property(COUNT).order(Order.Desc).build())
						.build())
				.operation(Limit.builder().num(config.getProduct().getFoodPairings().getLimit()).build()).build());
		results.forEach(r -> {
			String foodPairings = r.get(FOOD_PAIRINGS);
			if (foodPairings == null || foodPairings.isEmpty() || foodPairings.isBlank()) {
				return;
			}
			Arrays.stream(foodPairings.split("[,\\n]")).map(s -> clean(s)).filter(s -> s.split(" ").length <= 2)
					.forEach(food -> {
						commands.sugadd(config.getProduct().getFoodPairings().getIndex(), food, 1.0, true);
					});
		});
		log.info("Loaded {} food pairings", results.size());
	}

	private String clean(String food) {
		List<String> allWords = Stream.of(food.toLowerCase().split(" "))
				.collect(Collectors.toCollection(ArrayList<String>::new));
		allWords.removeAll(stopwords);
		String result = allWords.stream().collect(Collectors.joining(" ")).trim();
		if (result.endsWith(".")) {
			result = result.substring(0, result.length() - 1);
		}
		return result;
	}

}
