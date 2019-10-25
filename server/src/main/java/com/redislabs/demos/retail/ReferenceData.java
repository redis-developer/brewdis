package com.redislabs.demos.retail;

import static com.redislabs.demos.retail.Field.BREWERY_ICON;
import static com.redislabs.demos.retail.Field.BREWERY_ID;
import static com.redislabs.demos.retail.Field.BREWERY_NAME;
import static com.redislabs.demos.retail.Field.CATEGORY_ID;
import static com.redislabs.demos.retail.Field.CATEGORY_NAME;
import static com.redislabs.demos.retail.Field.COUNT;
import static com.redislabs.demos.retail.Field.PRODUCT_ID;
import static com.redislabs.demos.retail.Field.STYLE_ID;
import static com.redislabs.demos.retail.Field.STYLE_NAME;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.redislabs.demos.retail.BrewdisController.BrewerySuggestionPayload;
import com.redislabs.demos.retail.BrewdisController.Category;
import com.redislabs.demos.retail.BrewdisController.Style;
import com.redislabs.lettusearch.RediSearchCommands;
import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.aggregate.AggregateOptions;
import com.redislabs.lettusearch.aggregate.AggregateResults;
import com.redislabs.lettusearch.aggregate.Group;
import com.redislabs.lettusearch.aggregate.reducer.CountDistinct;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ReferenceData {

	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private BrewdisConfig config;
	@Getter
	private List<Category> categories;
	@Getter
	private Map<String, List<Style>> styles = new HashMap<>();

	@PostConstruct
	private void init() {
		RediSearchCommands<String, String> commands = connection.sync();
		AggregateResults<String, String> results = commands.aggregate(config.getProduct().getIndex(), "*",
				AggregateOptions.builder().load(CATEGORY_NAME)
						.operation(Group.builder().property(CATEGORY_ID).property(CATEGORY_NAME)
								.reduce(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
						.build());
		this.categories = results.stream()
				.map(r -> Category.builder().id(r.get(CATEGORY_ID)).name(r.get(CATEGORY_NAME)).build())
				.sorted(Comparator.comparing(Category::getName, Comparator.nullsLast(Comparator.naturalOrder())))
				.collect(Collectors.toList());
		this.categories.forEach(category -> {
			AggregateResults<String, String> styleResults = connection.sync()
					.aggregate(config.getProduct().getIndex(), tag(CATEGORY_ID, category.getId()),
							AggregateOptions.builder().load(STYLE_NAME).operation(Group.builder().property(STYLE_ID).property(STYLE_NAME)
									.reduce(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
									.build());
			List<Style> styleList = styleResults.stream()
					.map(r -> Style.builder().id(r.get(STYLE_ID)).name(r.get(STYLE_NAME)).build())
					.sorted(Comparator.comparing(Style::getName, Comparator.nullsLast(Comparator.naturalOrder())))
					.collect(Collectors.toList());
			this.styles.put(category.getId(), styleList);
		});
		loadBreweries();
	}

	private void loadBreweries() {
		AggregateResults<String, String> results = connection.sync().aggregate(config.getProduct().getIndex(), "*",
				AggregateOptions.builder().load(BREWERY_NAME).load(BREWERY_ICON)
						.operation(Group.builder().property(BREWERY_ID).property(BREWERY_NAME).property(BREWERY_ICON)
								.reduce(CountDistinct.builder().property(PRODUCT_ID).as(COUNT).build()).build())
						.build());
		ObjectMapper mapper = new ObjectMapper();
		results.forEach(r -> {
			BrewerySuggestionPayload payloadObject = BrewerySuggestionPayload.builder().id(r.get(BREWERY_ID))
					.icon(r.get(BREWERY_ICON)).build();
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
			connection.sync().sugadd(config.getProduct().getBrewerySuggestionIndex(), breweryName, count, payload);
		});
	}

	public String tag(String field, String value) {
		return "@" + field + ":{" + value + "}";
	}

}
