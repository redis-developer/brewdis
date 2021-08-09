package com.redislabs.demo.brewdis.web;

import com.redislabs.mesclun.search.SearchResults;
import lombok.Data;

@Data
public class ResultsPage {
    private long count;
    private SearchResults<String, String> results;
    private float duration;
    private long pageIndex;
    private long pageSize;
}
