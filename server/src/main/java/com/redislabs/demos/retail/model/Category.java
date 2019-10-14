package com.redislabs.demos.retail.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Category {

	private String id;
	private String name;

}
