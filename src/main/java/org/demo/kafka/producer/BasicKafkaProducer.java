package org.demo.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.demo.kafka.config.KafkaConfig;

public class BasicKafkaProducer {

	private final Producer<String, String> producer ;

	/**
	 * Constructor
	 * @param brokers
	 * @param username
	 * @param password
	 */
	public BasicKafkaProducer(String brokers, String username, String password) {
		super();
		this.producer = new KafkaProducer<>( KafkaConfig.getProducerProperties(brokers, username, password) );
	}

	/**
	 * Sends the given record
	 * @param kafkaRecord
	 */
	public void sendRecord(ProducerRecord<String, String> kafkaRecord ) {
		producer.send(kafkaRecord, (metadata, exception) -> {
				if ( exception != null ) {
					// if authentication failed => SaslAuthenticationException here
					System.out.println("=====> ERROR : Exception : " + exception.getMessage() );
				}
				else {
					System.out.println("=====> OK : METADATA : topic = " + metadata.topic() + " partition = " + metadata.partition() );
//					if ( metadata.hasOffset() ) {
//					metadata.offset();
//				}
				}
			
		});
		//producer.flush(); // useful ???
	}

	/**
	 * Closes the producer
	 */
	public void close() {
		producer.close();
	}

}
