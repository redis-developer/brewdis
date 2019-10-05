package com.redislabs.demos.retail.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class InventoryUpdate {

	private String store;
	private String sku;
	private Integer quantity;

}
