package org.demo.kafka.producer;

import java.time.Instant;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.demo.kafka.config.KafkaConfig;

public class BasicKafkaProducerOLD {

	public static void main(String[] args) {

		//Producer<String, String> producer = buildProducer("glider.srvs.cloudkafka.com:9094");
		Producer<String, String> producer = new KafkaProducer<>(KafkaConfig.getProperties(
				"glider.srvs.cloudkafka.com:9094", 
				// "glider.srvs.cloudkafka.com:9093",  // Not OK : loop  Found least loaded connecting node glider.srvs.cloudkafka.com:9093 (id: -1 rack: null)
				// To authenticate with SSL certificate, get your certificates here and connect to Kafka at port 9093
				"xxx", // put the user name here
				"xxx") // put the password here
				);

		// prepare the record
		String recordValue = "Current time is " + Instant.now().toString();
		System.out.println("Sending message: " + recordValue);
		ProducerRecord<String, String> record = new ProducerRecord<>("ieimnqtz-aaa", null, recordValue);

		// produce the record
		try {
			producer.send(record);
			producer.flush(); // useful ???
		} catch (Exception e) {
			e.printStackTrace();
		}

		// close the producer at the end
		producer.close();
	}

}
