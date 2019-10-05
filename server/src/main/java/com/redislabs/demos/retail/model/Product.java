package com.redislabs.demos.retail.model;

import org.springframework.data.annotation.Id;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Product {

	@Id
	private String sku;
	private String abv;
	private String category;
	private String description;
	private String label;
	private String name;
	private String organic;
	private String style;

}
