package com.redislabs.demos.retail;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.redislabs.demos.retail.RetailConfig.StompConfig;
import com.redislabs.demos.retail.model.Field;
import com.redislabs.lettusearch.search.SearchResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequiredArgsConstructor
@RequestMapping(path = "/api")
@CrossOrigin
@Slf4j
class RetailController {

	@Autowired
	private RetailConfig config;
	@Autowired
	private ProductService productService;
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	@Autowired
	private RedisUtils utils;

	@Autowired
	private SimpMessageSendingOperations sendingOps;

	@GetMapping("/config/stomp")
	public StompConfig stompConfig() {
		return config.getStomp();
	}

	@GetMapping("/products/search")
	public Stream<SearchResult<String, String>> productSearch(
			@RequestParam(name = "category", required = false) String category,
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

	@GetMapping("/inventory")
	public List<SearchResult<String, String>> inventory() {
		List<SearchResult<String, String>> results = productService.inventory();
		Map<String, Map<String, String>> inventory = new LinkedHashMap<>();
		for (SearchResult<String, String> result : results) {
			StreamMessageListenerContainerOptions<String, MapRecord<String, String, String>> containerOptions = StreamMessageListenerContainerOptions
					.builder().pollTimeout(Duration.ofMillis(1000)).build();
			StreamMessageListenerContainer<String, MapRecord<String, String, String>> container = StreamMessageListenerContainer
					.create(redisTemplate.getConnectionFactory(), containerOptions);
			container.start();
			String id = utils.key(result.get(Field.store.name()), result.get(Field.sku.name()));
			String stream = utils.key(config.getInventoryUpdatesStream(), id);
			inventory.put(id, result);
			Subscription subscription = container.receive(StreamOffset.latest(stream), m -> {
				inventory.put(id, m.getValue());
				log.info("received: {}", m.getValue());
				sendingOps.convertAndSend(config.getStomp().getInventoryTopic(), inventory.values());
			});
			try {
				subscription.await(Duration.ofSeconds(2));
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return results;
	}

}