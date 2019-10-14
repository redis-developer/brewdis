package com.redislabs.demos.retail;

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
	private InventoryUpdateGenerator generator;

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	@GetMapping("/search")
	public SearchResults<String, String> products(@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude,
			@RequestParam(name = "categoryId", required = false) String categoryId,
			@RequestParam(name = "styleId", required = false) String styleId,
			@RequestParam(name = "query", required = false) String query) {
		SearchResults<String, String> results = productService.searchProducts(categoryId, styleId, query);
		List<String> skus = results.stream().map(r -> r.get(Field.SKU)).collect(Collectors.toList());
		List<String> stores = productService.stores(longitude, latitude).stream().map(r -> r.get(Field.STORE))
				.collect(Collectors.toList());
		generator.generate(skus, stores);
		return results;
	}

	@GetMapping("/styles")
	public Stream<Style> styles(
			@RequestParam(name = "categoryId", defaultValue = "", required = false) String categoryId) {
		return productService.styles(categoryId);
	}

	@GetMapping("/categories")
	public Stream<Category> categories() {
		return productService.categories();
	}

	@GetMapping("/inventory")
	public SearchResults<String, String> inventory(@RequestParam(name = "store", required = false) String store) {
		return productService.searchInventory(store);
	}

	@GetMapping("/availability")
	public SearchResults<String, String> availability(@RequestParam(name = "sku", required = true) String sku,
			@RequestParam(name = "longitude", required = true) Double longitude,
			@RequestParam(name = "latitude", required = true) Double latitude) {
		return productService.availability(sku, longitude, latitude);
	}

}