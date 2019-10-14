package com.redislabs.demos.retail;

import java.util.Comparator;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

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

@Component
public class ProductService {

	@Autowired
	private RetailConfig config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	public SearchResults<String, String> searchInventory(String store) {
		String query = store == null ? "*" : "@store:{" + store + "}";
		return connection.sync().search(config.getInventoryIndex(), query,
				SearchOptions.builder().sortBy(SortBy.builder().field(Field.ID).build())
						.limit(Limit.builder().num(config.getMaxInventorySearchResults()).build()).build());
	}

	public SearchResults<String, String> searchProducts(String categoryId, String styleId, String keywords) {
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
		return connection.sync().search(config.getProductIndex(), query, SearchOptions.builder()
				.limit(Limit.builder().num(config.getMaxProductSearchResults()).build()).build());
	}

	private String tag(String field, String value) {
		String escapedField = field.replace(".", "\\.");
		String escapedValue = value.replace("-", "\\-").replace("(", "\\(").replace(")", "\\)");
		return "@" + escapedField + ":{'" + escapedValue + "'}";
	}

	public Stream<Style> styles(String categoryId) {
		AggregateResults<String, String> results = connection.sync()
				.aggregate("products", tag("style.category.id", categoryId),
						AggregateOptions.builder()
								.operation(Group.builder().property("style.name").property("style.id")
										.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
								.build());
		return results.stream().map(r -> Style.builder().id(r.get("style.id")).name(r.get("style.name")).build())
				.sorted(Comparator.comparing(Style::getName, Comparator.nullsLast(Comparator.reverseOrder())));
	}

	public Stream<Category> categories() {
		AggregateResults<String, String> results = connection.sync()
				.aggregate("products", "*",
						AggregateOptions.builder()
								.operation(Group.builder().property("style.category.name").property("style.category.id")
										.reduce(CountDistinct.builder().property("sku").as("count").build()).build())
								.build());
		return results.stream()
				.map(r -> Category.builder().name(r.get("style.category.name")).id(r.get("style.category.id")).build())
				.sorted(Comparator.comparing(Category::getName, Comparator.nullsLast(Comparator.reverseOrder())));
	}

	public SearchResults<String, String> availability(String sku, Double longitude, Double latitude) {
		String query = "@sku:{" + sku + "}";
		query += " " + geoCriteria(longitude, latitude);
		return connection.sync().search(config.getInventoryIndex(), query);
	}

	private String geoCriteria(Double longitude, Double latitude) {
		return "@location:[" + longitude + " " + latitude + " " + config.getAvailabilityRadius() + "]";
	}

	public SearchResults<String, String> stores(Double longitude, Double latitude) {
		String query = geoCriteria(longitude, latitude);
		return connection.sync().search(config.getStoreIndex(), query);
	}

}
