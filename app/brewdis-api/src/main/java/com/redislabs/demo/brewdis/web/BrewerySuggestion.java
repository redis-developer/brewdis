package com.redislabs.demo.brewdis.web;

import com.redislabs.mesclun.search.SearchResults;
import lombok.Builder;
import lombok.Data;

@Data
public class BrewerySuggestion {
    private String id;
    private String name;
    private String icon;


    @Data
    public static class Payload {

        private String id;
        private String icon;

    }

}
