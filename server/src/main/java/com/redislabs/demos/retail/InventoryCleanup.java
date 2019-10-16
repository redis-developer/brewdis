package com.redislabs.demos.retail;

import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.redislabs.lettusearch.StatefulRediSearchConnection;
import com.redislabs.lettusearch.search.Limit;
import com.redislabs.lettusearch.search.SearchOptions;
import com.redislabs.lettusearch.search.SearchResults;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class InventoryCleanup {
	@Autowired
	private StatefulRediSearchConnection<String, String> connection;
	@Autowired
	private RetailConfig config;

	@Scheduled(fixedRate = 60000)
	public void run() {
		log.info("Cleaning up inventory");
		ZonedDateTime time = ZonedDateTime.now(ZoneOffset.UTC)
				.minus(Duration.ofSeconds(config.getInventory().getCleanup().getAgeThreshold()));
		String query = "@epoch:[0 " + time.toEpochSecond() + "]";
		SearchResults<String, String> results = connection.sync().search(config.getInventory().getIndex(), query,
				SearchOptions.builder().noContent(true)
						.limit(Limit.builder().num(config.getInventory().getCleanup().getSearchLimit()).build())
						.build());
		results.forEach(r -> connection.sync().del(config.getInventory().getIndex(), r.getDocumentId(), true));
		if (results.size() > 0) {
			log.info("Deleted {} docs from {} index", results.size(), config.getInventory().getIndex());
		}
	}
}