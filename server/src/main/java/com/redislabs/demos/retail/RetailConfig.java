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
	private String availabilityRadius = "25 mi";
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
		private String outputStream = "inventory:out";
		private String inputStream = "inventory:in";
		private String index = "inventory";
		private String keyspace = "inventory";
		private int searchLimit = 1000;
		private int levelLow = 10;
		private int levelMedium = 20;
		private InventoryGeneratorConfig generator = new InventoryGeneratorConfig();
		private InventoryRestockConfig restock = new InventoryRestockConfig();
		private InventoryCleanupConfig cleanup = new InventoryCleanupConfig();

		public String level(int quantity) {
			if (quantity <= levelLow) {
				return "low";
			}
			if (quantity <= levelMedium) {
				return "medium";
			}
			return "high";
		}

	}

	@Data
	public static class InventoryRestockConfig {
		private int delayMin = 5;
		private int delayMax = 30;
		private int threshold = 5;
		private int deltaMin = 20;
		private int deltaMax = 90;
	}

	@Data
	public static class InventoryCleanupConfig {
		private int searchLimit = 10000;
		private long ageThreshold = 3600;
	}

	@Data
	public static class InventoryGeneratorConfig {
		private int onHandMin = 40;
		private int onHandMax = 100;
		private int deltaMin = -3;
		private int deltaMax = -1;
		private int allocatedMin = 1;
		private int allocatedMax = 10;
		private int reservedMin = 1;
		private int reservedMax = 10;
		private int virtualHoldMin = 1;
		private int virtualHoldMax = 10;
		private long requestDurationInMin = 10;
		private int maxSkus = 5;
		private int maxStores = 5;
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