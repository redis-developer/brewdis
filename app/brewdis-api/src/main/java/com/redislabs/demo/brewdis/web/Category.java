package com.redislabs.demo.brewdis.web;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class Category {

    private String id;
    private String name;

}
