package com.redislabs.demo.brewdis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@ConfigurationProperties(prefix = "")
@EnableAutoConfiguration
@Data
public class BrewdisConfig {

	private String keySeparator;
	private long streamPollTimeout = 100;
	private StompConfig stomp = new StompConfig();
	private String availabilityRadius;
	private ProductConfig product = new ProductConfig();
	private StoreConfig store = new StoreConfig();
	private InventoryConfig inventory = new InventoryConfig();
	private SessionConfig session = new SessionConfig();

	@Data
	public static class SessionConfig {
		private String cartAttribute = "cart";
		private String coordsAttribute = "coords";
	}

	@Data
	public static class StoreConfig {
		private String index;
		private String keyspace;
		private String url;
		private Map<String, String> inventoryMapping = new HashMap<>();
	}

	@Data
	public static class ProductConfig {
		private String index;
		private String keyspace;
		private String brewerySuggestionIndex;
		private boolean brewerySuggestIndexFuzzy;
		private String url;
		private Map<String, String> inventoryMapping = new HashMap<>();
	}

	@Data
	public static class InventoryConfig {
		private String stream;
		private String index;
		private String keyspace;
		private int searchLimit;
		private int levelLow;
		private int levelMedium;
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
		private int delayMin;
		private int delayMax;
		private int threshold;
		private int deltaMin;
		private int deltaMax;
	}

	@Data
	public static class InventoryCleanupConfig {
		private int searchLimit;
		private long ageThreshold;
		private long streamTrimCount;
	}

	@Data
	public static class InventoryGeneratorConfig {
		private String stream;
		private int onHandMin;
		private int onHandMax;
		private int deltaMin;
		private int deltaMax;
		private int allocatedMin;
		private int allocatedMax;
		private int reservedMin;
		private int reservedMax;
		private int virtualHoldMin;
		private int virtualHoldMax;
		private long requestDurationInMin;
		private int maxStores;
	}

	@Data
	public static class StompConfig implements Serializable {
		private static final long serialVersionUID = -623741573410463326L;
		private String protocol;
		private String host;
		private int port;
		private String endpoint;
		private String destinationPrefix;
		private String inventoryTopic;
	}

	public String concat(String... keys) {
		return String.join(keySeparator, keys);
	}

	public String tag(String field, String value) {
		return "@" + field + ":{" + value + "}";
	}

}