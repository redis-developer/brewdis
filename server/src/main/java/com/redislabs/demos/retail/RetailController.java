package com.redislabs.demos.retail;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.redislabs.demos.retail.RetailConfig.StompConfig;
import com.redislabs.lettusearch.search.SearchResult;
import com.redislabs.lettusearch.search.SearchResults;

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
	private ProductService productService;
	@Autowired
	private InventoryGenerator generator;

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	@GetMapping("/products/search")
	public Stream<SearchResult<String, String>> productSearch(
			@RequestParam(name = "category", required = false) String category,
			@RequestParam(name = "style", required = false) String style,
			@RequestParam(name = "query", required = false) String query) {
		SearchResults<String, String> results = productService.search(category, style, query);
		List<SearchResult<String, String>> list = results.stream()
				.filter(r -> r.containsKey("labels.contentAwareMedium")).collect(Collectors.toList());
		List<String> skus = list.stream().map(r -> r.get(Field.SKU)).collect(Collectors.toList());
		generator.setSkus(skus);
		return list.stream();
	}

	@GetMapping("/products/styles")
	public Stream<String> productStyles(
			@RequestParam(name = "prefix", defaultValue = "", required = false) String prefix) {
		return productService.styles(prefix);
	}

	@GetMapping("/products/categories")
	public Set<String> productCategories() {
		return productService.categories();
	}

	@GetMapping("/inventory")
	public SearchResults<String, String> inventory() {
		return productService.inventory();
	}

}