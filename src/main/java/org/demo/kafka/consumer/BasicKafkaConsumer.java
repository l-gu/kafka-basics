package org.demo.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.demo.kafka.config.KafkaConfig;

public class BasicKafkaConsumer {

	private final Consumer<String, String> consumer ;

	public BasicKafkaConsumer(String brokers, String username, String password, int maxPollRecords) {
		super();
		this.consumer = new KafkaConsumer<>( KafkaConfig.getConsumerProperties(brokers, username, password, maxPollRecords) );
	}
	
	public void subscribe(String topic) {
		// connect the consumer to the specified topic
		consumer.subscribe(Arrays.asList(topic));
	}

	public ConsumerRecords<String, String> poll(Duration duration) {
		// The poll method is not thread safe and is not meant to get called from multiple threads.
		// You can can control the maximum records returned by the poll() with props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
		return consumer.poll(duration);
	}

	public void close() {
		consumer.close();
	}

	public void commitAsync() {
		consumer.commitAsync();
	}

}
