package com.redislabs.demos.retail;

import java.util.Set;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.redislabs.demos.retail.RetailConfig.StompConfig;
import com.redislabs.lettusearch.search.SearchResult;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = "/api")
@CrossOrigin
class RetailController {

	@Autowired
	private RetailConfig config;
	@Autowired
	private ProductService productService;

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	@GetMapping("/products/search")
	public Stream<SearchResult<String, String>> productSearch(@RequestParam(name = "category", required = false) String category,
			@RequestParam(name = "style", required = false) String style,
			@RequestParam(name = "query", required = false) String query) {
		return productService.search(category, style, query);
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

}