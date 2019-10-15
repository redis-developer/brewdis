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

	private String keySeparator = ":";
	private StompConfig stomp = new StompConfig();
	private String availabilityRadius = "100 mi";
	private ProductConfig product = new ProductConfig();
	private StoreConfig store = new StoreConfig();
	private InventoryConfig inventory = new InventoryConfig();

	@Data
	public static class StoreConfig {
		private String index = "stores";
		private String keyspace = "store";
	}

	@Data
	public static class ProductConfig {
		private String index = "products";
		private String keyspace = "product";
		private int searchLimit = 50;
	}

	@Data
	public static class InventoryConfig {
		private String stream = "inventory-stream";
		private String index = "inventory";
		private String keyspace = "inventory";
		private int searchLimit = 1000;
		private InventoryGeneratorConfig generator = new InventoryGeneratorConfig();
	}

	@Data
	public static class InventoryGeneratorConfig {
		private int restockingQuantityMin = 30;
		private int restockingQuantityMax = 100;
		private int restockingDelayMin = 10;
		private int restockingDelayMax = 50;
		private int deltaMin = -10;
		private int deltaMax = -1;
		private long sleep = 50;
		private long cleanupPeriod = 30;
		public int cleanupLimit = 10000;
	}

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