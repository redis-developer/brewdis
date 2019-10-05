package com.redislabs.demos.retail.model;

import org.springframework.data.annotation.Id;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Data;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Inventory {

	@Id
	private String store;
	@Id
	private String sku;
	private String abv;
	private String address;
	private String address2;
	private String address3;
	private String availableToSell;
	private String city;
	private String country;
	private String isDefaultStore;
	private String isPreferredStore;
	private String latitude;
	private String location;
	private String longitude;
	private String market;
	private String organic;
	private String parentDc;
	private String productCategory;
	private String productDescription;
	private String productName;
	private String productStyle;
	@Default
	private String quantity = "0";
	private String rollupInventory;
	private String state;
	private String storeDescription;
	private String storeType;
	private String zip;
}
