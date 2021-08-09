package com.redislabs.demo.brewdis;

import java.time.Duration;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer.StreamMessageListenerContainerOptions;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.messaging.simp.SimpMessageSendingOperations;
import org.springframework.stereotype.Component;

@Component
public class WebSocketPublisher implements InitializingBean, DisposableBean, StreamListener<String, MapRecord<String, String, String>> {

	@Autowired
	private Config config;
	@Autowired
	private StringRedisTemplate template;
	@Autowired
	private SimpMessageSendingOperations sendingOps;
	private StreamMessageListenerContainer<String, MapRecord<String, String, String>> container;
	private Subscription subscription;

	@Override
	public void afterPropertiesSet() throws Exception {
		this.container = StreamMessageListenerContainer.create(template.getConnectionFactory(),
				StreamMessageListenerContainerOptions.builder()
						.pollTimeout(Duration.ofMillis(config.getStreamPollTimeout())).build());
		container.start();
		this.subscription = container.receive(StreamOffset.latest(config.getInventory().getStream()), this);
		subscription.await(Duration.ofSeconds(2));
	}

	@Override
	public void destroy() throws Exception {
		if (subscription != null) {
			subscription.cancel();
		}
		if (container != null) {
			container.stop();
		}
	}

	@Override
	public void onMessage(MapRecord<String, String, String> message) {
		sendingOps.convertAndSend(config.getStomp().getInventoryTopic(), message.getValue());
	}

}
