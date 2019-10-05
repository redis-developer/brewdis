package com.redislabs.rediscogs;

import org.ruaux.jdiscogs.JDiscogsConfiguration;
import org.ruaux.jdiscogs.data.BatchConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

import com.redislabs.springredisearch.RediSearchConfiguration;

@SpringBootApplication(scanBasePackageClasses = { RediscogsProperties.class, RediSearchConfiguration.class,
		JDiscogsConfiguration.class })
@EnableCaching
public class RediscogsApplication implements ApplicationRunner {

	@Autowired
	private BatchConfiguration batch;
	@Autowired
	private LikeConsumer likeConsumer;

	public static void main(String[] args) {
		SpringApplication.run(RediscogsApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		batch.runJobs();
		likeConsumer.start();
	}

}
