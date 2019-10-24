package com.redislabs.demos.retail;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.redislabs.springredisearch.RediSearchConfiguration;

@SpringBootApplication(scanBasePackageClasses = { BrewdisApplication.class, RediSearchConfiguration.class })
@EnableScheduling
public class BrewdisApplication {

	public static void main(String[] args) {
		SpringApplication.run(BrewdisApplication.class, args);
	}

}
