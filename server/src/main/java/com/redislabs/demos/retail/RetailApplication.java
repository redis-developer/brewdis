package com.redislabs.demos.retail;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.redislabs.springredisearch.RediSearchConfiguration;

@SpringBootApplication(scanBasePackageClasses = { RetailApplication.class, RediSearchConfiguration.class })
@EnableScheduling
public class RetailApplication {

	public static void main(String[] args) {
		SpringApplication.run(RetailApplication.class, args);
	}

}
