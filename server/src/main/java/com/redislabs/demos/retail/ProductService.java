package com.redislabs.demos.retail;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResult;
import com.redislabs.lettusearch.search.SearchResults;
import com.redislabs.lettusearch.suggest.SuggestGetOptions;
import com.redislabs.lettusearch.suggest.SuggestResult;

@Component
public class ProductService {

	@Autowired
	private RetailConfig config;
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;

	public List<SearchResult<String, String>> inventory() {
		return connection.sync().search(config.getInventoryIndex(), "*");
	}

	public Stream<SearchResult<String, String>> search(String category, String style, String keywords) {
		String query = "";
		if (category != null && category.length() > 0) {
			query += tag("style.category.name", category);
		}
		if (style != null && style.length() > 0) {
			query += " " + tag("style.name", style);
		}
		if (keywords != null && keywords.length() > 0) {
			query += " " + keywords;
		}
		if (query.length() == 0) {
			query = "*";
		}
		SearchResults<String, String> results = connection.sync().search(config.getProductIndex(), query,
				SearchOptions.builder().limit(Limit.builder().num(config.getSearchResultsLimit()).build()).build());
		return results.stream();
	}

	private String tag(String field, String value) {
		String escapedField = field.replace(".", "\\.");
		String escapedValue = value.replace("-", "\\-");
		return "@" + escapedField + ":{'" + escapedValue + "'}";
	}

	public Stream<String> styles(String prefix) {
		List<SuggestResult<String>> results = connection.sync().sugget(config.getStyleSuggestionIndex(), prefix,
				SuggestGetOptions.builder().max(100l).fuzzy(config.isFuzzySuggest()).build());
		return results.stream().map(s -> s.getString());
	}

	public Set<String> categories() {
		return connection.sync().smembers(config.getCategoriesKey());
	}

}
