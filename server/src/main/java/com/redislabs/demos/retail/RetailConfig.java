package com.redislabs.demos.retail;

import java.io.Serializable;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class RetailConfig {

	private int maxInventorySearchResults = 100;
	private int maxProductSearchResults = 30;
	private String productIndex = "products";
	private String productKeyspace = "product";
	private String storeIndex = "stores";
	private String storeKeyspace = "store";
	private String inventoryUpdatesStream = "inventory-updates";
	private String inventoryIndex = "inventory";
	private String inventoryKeyspace = "inventory";
	private String styleSuggestionIndex = "styles";
	private String categoriesKey = "categories";
	private boolean fuzzySuggest;
	private String keySeparator = ":";
	private StompConfig stomp = new StompConfig();
	private long generatorSleep = 100;
	private int generatorSkuCount = 10;
	private long generatorDuration = 1200;
	private String availabilityRadius = "100 mi";
	private int inventoryRestockingQuantity = 50;
	private int inventoryRestockingDelay = 10;
	private int inventoryDeltaMin = -10;
	private int inventoryDeltaMax = -1;

	@Data
	public static class StompConfig implements Serializable {
		private static final long serialVersionUID = -623741573410463326L;
		private String protocol = "ws";
		private String host = "localhost";
		private int port = 8080;
		private String endpoint = "/websocket";
		private String destinationPrefix = "/topic";
		private String inventoryTopic = destinationPrefix + "/inventory";
	}

}