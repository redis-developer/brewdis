package com.redislabs.demo.brewdis.web;

import com.redislabs.mesclun.search.SearchResults;
import lombok.Data;

@Data
public class Query {
    private String query = "*";
    private String sortByField;
    private String sortByDirection = "Ascending";
    private long pageIndex = 0;
    private long pageSize = 100;

    public long getOffset() {
        return pageIndex * pageSize;
    }

}


