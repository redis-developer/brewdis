package com.redislabs.demo.brewdis;

import static com.redislabs.demo.brewdis.Field.AVAILABLE_TO_PROMISE;
import static com.redislabs.demo.brewdis.Field.BREWERY_NAME;
import static com.redislabs.demo.brewdis.Field.CATEGORY_NAME;
import static com.redislabs.demo.brewdis.Field.LEVEL;
import static com.redislabs.demo.brewdis.Field.LOCATION;
import static com.redislabs.demo.brewdis.Field.PRODUCT_DESCRIPTION;
import static com.redislabs.demo.brewdis.Field.PRODUCT_ID;
import static com.redislabs.demo.brewdis.Field.PRODUCT_NAME;
import static com.redislabs.demo.brewdis.Field.STORE_ID;
import static com.redislabs.demo.brewdis.Field.STYLE_NAME;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpSession;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.demo.brewdis.BrewdisConfig.StompConfig;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
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
	@Autowired
	private DataLoader data;
	private ObjectMapper mapper = new ObjectMapper();

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	public static @Data class Query {
		private String query = "*";
		private String sortByField;
		private String sortByDirection = "Ascending";
		private long limit = 100;
		private long offset = 0;
	}

	public static @Data class TimedSearchResults {
		private long count;
		private SearchResults<String, String> results;
		private float duration;
	}

	@PostMapping("/products")
	public TimedSearchResults products(@RequestBody Query query,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude, HttpSession session) {
		SearchOptionsBuilder options = SearchOptions.builder()
				.highlight(HighlightOptions.builder().field(PRODUCT_NAME).field(PRODUCT_DESCRIPTION)
						.field(CATEGORY_NAME).field(STYLE_NAME).field(BREWERY_NAME)
						.tags(TagOptions.builder().open("<mark>").close("</mark>").build()).build())
				.limit(Limit.builder().offset(query.getOffset()).num(query.getLimit()).build());
		if (query.getSortByField() != null) {
			options.sortBy(SortBy.builder().field(query.getSortByField())
					.direction(Direction.valueOf(query.getSortByDirection())).build());
		}
		String queryString = query.getQuery() == null || query.getQuery().length() == 0 ? "*" : query.getQuery();
		long startTime = System.currentTimeMillis();
		SearchResults<String, String> results = connection.sync().search(config.getProduct().getIndex(), queryString,
				options.build());
		long endTime = System.currentTimeMillis();
		List<String> skus = results.stream().map(r -> r.get(PRODUCT_ID)).collect(Collectors.toList());
		List<String> stores = connection.sync().search(config.getStore().getIndex(), geoCriteria(longitude, latitude))
				.stream().map(r -> r.get(STORE_ID)).collect(Collectors.toList());
		generator.add(session.getId(), stores, skus);
		TimedSearchResults resultsWithCount = new TimedSearchResults();
		resultsWithCount.setCount(results.getCount());
		resultsWithCount.setResults(results);
		resultsWithCount.setDuration(((float) (endTime - startTime)) / 1000);
		return resultsWithCount;
	}

	@Builder
	public static @Data class Style {

		private String id;
		private String name;

	}

	@GetMapping("/styles")
	public List<Style> styles(@RequestParam(name = "category", defaultValue = "", required = false) String category) {
		return data.getStyles().get(category);
	}

	@Builder
	public static @Data class Category {

		private String id;
		private String name;

	}

	@GetMapping("/categories")
	public List<Category> categories() {
		return data.getCategories();
	}

	@GetMapping("/inventory")
	public SearchResults<String, String> inventory(@RequestParam(name = "store", required = false) String store) {
		String query = "@" + AVAILABLE_TO_PROMISE + ":[0 inf]";
		if (store != null) {
			query += " " + config.tag(STORE_ID, store);
		}
		return connection.sync().search(config.getInventory().getIndex(), query,
				SearchOptions.builder().sortBy(SortBy.builder().field(STORE_ID).field(PRODUCT_ID).build())
						.limit(Limit.builder().num(config.getInventory().getSearchLimit()).build()).build());
	}

	@GetMapping("/availability")
	public SearchResults<String, String> availability(@RequestParam(name = "sku", required = false) String sku,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude) {
		String query = geoCriteria(longitude, latitude);
		if (sku != null) {
			query += " " + config.tag(PRODUCT_ID, sku);
		}
		SearchResults<String, String> results = connection.sync().search(config.getInventory().getIndex(), query,
				SearchOptions.builder().limit(Limit.builder().num(config.getInventory().getSearchLimit()).build())
						.build());
		results.forEach(r -> {
			String atpString = r.get(AVAILABLE_TO_PROMISE);
			if (atpString == null) {
				return;
			}
			int availableToPromise = Integer.parseInt(atpString);
			r.put(LEVEL, config.getInventory().level(availableToPromise));
		});
		return results;

	}

	private String geoCriteria(Double longitude, Double latitude) {
		return "@" + LOCATION + ":[" + longitude + " " + latitude + " " + config.getAvailabilityRadius() + "]";
	}

	public static @Data class BrewerySuggestion {
		private String id;
		private String name;
		private String icon;
	}

	public static @Data class BrewerySuggestionPayload {
		private String id;
		private String icon;
	}

	@GetMapping("/breweries/suggest")
	public Stream<BrewerySuggestion> suggestBreweries(
			@RequestParam(name = "prefix", defaultValue = "", required = false) String prefix) {
		List<SuggestResult<String>> results = connection.sync().sugget(config.getProduct().getBrewerySuggestionIndex(),
				prefix, SuggestGetOptions.builder().withPayloads(true).max(20l)
						.fuzzy(config.getProduct().isBrewerySuggestIndexFuzzy()).build());
		return results.stream().map(s -> {
			BrewerySuggestion suggestion = new BrewerySuggestion();
			suggestion.setName(s.getString());
			BrewerySuggestionPayload payload;
			try {
				payload = mapper.readValue(s.getPayload(), BrewerySuggestionPayload.class);
				suggestion.setId(payload.getId());
				suggestion.setIcon(payload.getIcon());
			} catch (Exception e) {
				log.error("Could not deserialize brewery payload {}", s.getPayload(), e);
			}
			return suggestion;
		});

	}

}