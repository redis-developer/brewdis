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

	private int searchResultsLimit = 300;
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
	private long generatorSleep = 10;
	private int generatorDeltaMin = -10;
	private int generatorDeltaMax = 10;

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