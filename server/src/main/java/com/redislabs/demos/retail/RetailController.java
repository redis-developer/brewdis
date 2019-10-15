package com.redislabs.demos.retail;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.redislabs.demos.retail.RetailConfig.StompConfig;
import com.redislabs.demos.retail.model.Category;
import com.redislabs.demos.retail.model.Style;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateResults;
import com.redislabs.lettusearch.aggregate.Group;
import com.redislabs.lettusearch.aggregate.reducer.CountDistinct;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.SortBy;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = "/api")
@CrossOrigin
//@Slf4j
class RetailController {

	@Autowired
	private RetailConfig config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private InventoryGenerator generator;

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	@GetMapping("/search")
	public SearchResults<String, String> products(
			@RequestParam(name = "categoryId", required = false) String categoryId,
			@RequestParam(name = "styleId", required = false) String styleId,
			@RequestParam(name = "query", required = false) String keywords,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude) {
		String query = "";
		if (categoryId != null && categoryId.length() > 0) {
			query += tag("style.category.id", categoryId);
		}
		if (styleId != null && styleId.length() > 0) {
			query += " " + tag("style.id", styleId);
		}
		if (keywords != null && keywords.length() > 0) {
			query += " " + keywords;
		}
		if (query.length() == 0) {
			query = "*";
		}
		SearchResults<String, String> results = connection.sync().search(config.getProduct().getIndex(), query,
				SearchOptions.builder().limit(Limit.builder().num(config.getProduct().getSearchLimit()).build())
						.build());
		List<String> skus = results.stream().map(r -> r.get("sku")).collect(Collectors.toList());
		List<String> stores = connection.sync().search(config.getStore().getIndex(), geoCriteria(longitude, latitude))
				.stream().map(r -> r.get("store")).collect(Collectors.toList());
		generator.generate(skus, stores);
		return results;

	}

	@GetMapping("/styles")
	public Stream<Style> styles(
			@RequestParam(name = "categoryId", defaultValue = "", required = false) String categoryId) {
		AggregateResults<String, String> results = connection.sync()
				.aggregate("products", tag("style.category.id", categoryId),
						AggregateOptions.builder()
								.operation(Group.builder().property("style.name").property("style.id")
										.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
								.build());
		return results.stream().map(r -> Style.builder().id(r.get("style.id")).name(r.get("style.name")).build())
				.sorted(Comparator.comparing(Style::getName, Comparator.nullsLast(Comparator.reverseOrder())));

	}

	@GetMapping("/categories")
	public Stream<Category> categories() {
		AggregateResults<String, String> results = connection.sync()
				.aggregate("products", "*",
						AggregateOptions.builder()
								.operation(Group.builder().property("style.category.name").property("style.category.id")
										.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
								.build());
		return results.stream()
				.map(r -> Category.builder().name(r.get("style.category.name")).id(r.get("style.category.id")).build())
				.sorted(Comparator.comparing(Category::getName, Comparator.nullsLast(Comparator.naturalOrder())));
	}

	@GetMapping("/inventory")
	public SearchResults<String, String> inventory(@RequestParam(name = "store", required = false) String store) {
		String query = store == null ? "*" : "@store:{" + store + "}";
		return connection.sync().search(config.getInventory().getIndex(), query,
				SearchOptions.builder().sortBy(SortBy.builder().field(Field.ID).build())
						.limit(Limit.builder().num(config.getInventory().getSearchLimit()).build()).build());
	}

	@GetMapping("/availability")
	public SearchResults<String, String> availability(@RequestParam(name = "sku", required = true) String sku,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude) {
		String query = "@sku:{" + sku + "}";
		query += " " + geoCriteria(longitude, latitude);
		return connection.sync().search(config.getInventory().getIndex(), query);
	}

	private String tag(String field, String value) {
		String escapedField = field.replace(".", "\\.");
		return "@" + escapedField + ":{'" + value + "'}";
	}

	private String geoCriteria(Double longitude, Double latitude) {
		return "@location:[" + longitude + " " + latitude + " " + config.getAvailabilityRadius() + "]";
	}

}