package com.redislabs.demos.retail;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RedisUtils {

	@Autowired
	private RetailConfig config;

	public String key(String... keys) {
		return String.join(config.getKeySeparator(), keys);
	}
}
