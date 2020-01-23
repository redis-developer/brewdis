package com.redislabs.demo.brewdis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BrewdisApplication implements ApplicationRunner {

	@Autowired
	private DataLoader loader;

	public static void main(String[] args) {
		SpringApplication.run(BrewdisApplication.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		loader.execute();
	}

}
