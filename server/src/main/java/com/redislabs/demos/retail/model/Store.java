package com.redislabs.demos.retail.model;

import org.springframework.data.annotation.Id;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class Store {

	@Id
	private String id;
	private String address;
	private String address2;
	private String address3;
	private String availableToSell;
	private String city;
	private String country;
	private String description;
	private String isDefault;
	private String isPreferred;
	private String latitude;
	private String location;
	private String longitude;
	private String market;
	private String parentDc;
	private String rollupInventory;
	private String state;
	private String type;
	private String zip;
}
