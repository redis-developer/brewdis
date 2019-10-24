package com.redislabs.demos.retail;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.geo.Point;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.demos.retail.BrewdisConfig.StompConfig;
import com.redislabs.demos.retail.BrewdisController.BrewerySuggestion.BrewerySuggestionBuilder;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateResults;
import com.redislabs.lettusearch.aggregate.Group;
import com.redislabs.lettusearch.aggregate.reducer.CountDistinct;
import com.redislabs.lettusearch.search.Direction;
import com.redislabs.lettusearch.search.HighlightOptions;
import com.redislabs.lettusearch.search.HighlightOptions.TagOptions;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchOptions.SearchOptionsBuilder;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.search.SortBy;
import com.redislabs.lettusearch.suggest.SuggestGetOptions;
import com.redislabs.lettusearch.suggest.SuggestResult;

import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = "/api")
@CrossOrigin
@Slf4j
class BrewdisController {

	@Autowired
	private BrewdisConfig config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private InventoryGenerator generator;
	private ObjectMapper mapper = new ObjectMapper();

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	@Data
	public static class Query {
		private String query = "*";
		private String sortByField;
		private String sortByDirection = "Ascending";
		private long limit = 100;
	}

	@PostMapping("/products")
	public SearchResults<String, String> products(@RequestBody Query query, HttpSession session) {
		SearchOptionsBuilder options = SearchOptions.builder()
				.highlight(HighlightOptions.builder().field("description")
						.tags(TagOptions.builder().open("<mark>").close("</mark>").build()).build())
				.limit(Limit.builder().num(query.getLimit()).build());
		if (query.getSortByField() != null) {
			options.sortBy(SortBy.builder().field(query.getSortByField())
					.direction(Direction.valueOf(query.getSortByDirection())).build());
		}
		String queryString = query.getQuery() == null || query.getQuery().length() == 0 ? "*" : query.getQuery();
		SearchResults<String, String> results = connection.sync().search(config.getProduct().getIndex(), queryString,
				options.build());
		@SuppressWarnings("unchecked")
		Set<String> cart = (Set<String>) session.getAttribute(config.getSession().getCartAttribute());
		if (cart != null) {
			results.forEach(r -> r.put(Field.ADDED, String.valueOf(cart.contains(r.get(Field.SKU)))));
		}
		return results;
	}

	@Data
	@Builder
	public static class Style {

		private String id;
		private String name;

	}

	@GetMapping("/styles")
	public Stream<Style> styles(@RequestParam(name = "category", defaultValue = "", required = false) String category) {
		AggregateResults<String, String> results = connection.sync()
				.aggregate("products", tag("category", category),
						AggregateOptions.builder()
								.operation(Group.builder().property("style").property("styleName")
										.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
								.build());
		return results.stream().map(r -> Style.builder().id(r.get("style")).name(r.get("styleName")).build())
				.sorted(Comparator.comparing(Style::getName, Comparator.nullsLast(Comparator.reverseOrder())));

	}

	@Data
	@Builder
	public static class Category {

		private String id;
		private String name;

	}

	@GetMapping("/categories")
	public Stream<Category> categories() {
		AggregateResults<String, String> results = connection.sync()
				.aggregate("products", "*",
						AggregateOptions.builder()
								.operation(Group.builder().property("category").property("categoryName")
										.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
								.build());
		return results.stream().map(r -> Category.builder().id(r.get("category")).name(r.get("categoryName")).build())
				.sorted(Comparator.comparing(Category::getName, Comparator.nullsLast(Comparator.naturalOrder())));
	}

	@GetMapping("/inventory")
	public SearchResults<String, String> inventory(@RequestParam(name = "store", required = false) String store) {
		String query = store == null ? "*" : "@store:{" + store + "}";
		return connection.sync().search(config.getInventory().getIndex(), query,
				SearchOptions.builder().sortBy(SortBy.builder().field(Field.STORE).field(Field.SKU).build())
						.limit(Limit.builder().num(config.getInventory().getSearchLimit()).build()).build());
	}

	@SuppressWarnings("unchecked")
	@GetMapping("/availability")
	public SearchResults<String, String> availability(@RequestParam(name = "sku", required = false) String sku,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude, HttpSession session) {
		String query = geoCriteria(longitude, latitude);
		List<String> skus = new ArrayList<>();
		if (sku == null) {
			Set<String> cart = (Set<String>) session.getAttribute(config.getSession().getCartAttribute());
			if (cart != null) {
				skus.addAll(cart);
			}
		} else {
			skus.add(sku);
		}
		if (!skus.isEmpty()) {
			query += " " + skuTagCriteria(skus);
		}
		SearchResults<String, String> results = connection.sync().search(config.getInventory().getIndex(), query,
				SearchOptions.builder()
						.limit(Limit.builder().num(config.getInventory().getGenerator().getMaxStores()).build())
						.build());
		results.forEach(r -> {
			String atpString = r.get(Field.AVAILABLE_TO_PROMISE);
			if (atpString == null) {
				return;
			}
			int availableToPromise = Integer.parseInt(atpString);
			r.put(Field.LEVEL, config.getInventory().level(availableToPromise));
		});
		return results;

	}

	private String skuTagCriteria(List<String> skus) {
		return "@" + Field.SKU + ":{" + String.join(" | ", skus) + "}";
	}

	@GetMapping("/cart")
	@SuppressWarnings("unchecked")
	public ResponseEntity<Void> addProduct(@RequestParam(name = "sku", required = false) String sku,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude, HttpSession session) {
		Set<String> cart = (Set<String>) session.getAttribute(config.getSession().getCartAttribute());
		if (cart == null) {
			cart = new LinkedHashSet<>();
		}
		cart.add(sku);
		session.setAttribute(config.getSession().getCartAttribute(), cart);
		Point coords = (Point) session.getAttribute(config.getSession().getCoordsAttribute());
		if (coords == null) {
			coords = new Point(longitude, latitude);
			session.setAttribute(config.getSession().getCoordsAttribute(), coords);
		}
		List<String> stores = connection.sync().search(config.getStore().getIndex(), geoCriteria(longitude, latitude))
				.stream().map(r -> r.get("store")).collect(Collectors.toList());
		generator.add(stores.subList(0, Math.min(config.getInventory().getGenerator().getMaxStores(), stores.size())),
				sku);
		return new ResponseEntity<>(HttpStatus.OK);

	}

	private String tag(String field, String value) {
		String escapedField = field.replace(".", "\\.");
		return "@" + escapedField + ":{'" + value + "'}";
	}

	private String geoCriteria(Double longitude, Double latitude) {
		return "@location:[" + longitude + " " + latitude + " " + config.getAvailabilityRadius() + "]";
	}

	@Data
	@Builder
	public static class BrewerySuggestion {
		private String id;
		private String name;
		private String icon;
	}

	@Data
	@Builder
	public static class BrewerySuggestionPayload {
		private String id;
		private String icon;
	}

	@GetMapping("/breweries/suggest")
	public Stream<BrewerySuggestion> suggestBreweries(
			@RequestParam(name = "prefix", defaultValue = "", required = false) String prefix) {
		if (connection.sync().suglen(config.getProduct().getBrewerySuggestionIndex()) == 0) {
			AggregateResults<String, String> results = connection.sync().aggregate("products", "*",
					AggregateOptions.builder().load("breweryIcon")
							.operation(Group.builder().property("brewery").property("breweryName")
									.property("breweryIcon")
									.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
							.build());
			results.forEach(r -> {
				BrewerySuggestionPayload payloadObject = BrewerySuggestionPayload.builder().id(r.get("brewery"))
						.icon(r.get("breweryIcon")).build();
				String payload = null;
				try {
					payload = mapper.writeValueAsString(payloadObject);
				} catch (JsonProcessingException e) {
					log.error("Could not serialize brewery payload {}", payloadObject, e);
				}
				String breweryName = r.get("breweryName");
				if (breweryName == null) {
					return;
				}
				double count = Double.parseDouble(r.get("count"));
				connection.sync().sugadd(config.getProduct().getBrewerySuggestionIndex(), breweryName, count, payload);
			});
		}
		List<SuggestResult<String>> results = connection.sync().sugget(config.getProduct().getBrewerySuggestionIndex(),
				prefix, SuggestGetOptions.builder().withPayloads(true).max(20l)
						.fuzzy(config.getProduct().isBrewerySuggestIndexFuzzy()).build());
		return results.stream().map(s -> {
			BrewerySuggestionBuilder builder = BrewerySuggestion.builder().name(s.getString());
			BrewerySuggestionPayload payload;
			try {
				payload = mapper.readValue(s.getPayload(), BrewerySuggestionPayload.class);
				builder.id(payload.getId()).icon(payload.getIcon());
			} catch (Exception e) {
				log.error("Could not deserialize brewery payload {}", s.getPayload(), e);
			}
			return builder.build();
		});

	}

}