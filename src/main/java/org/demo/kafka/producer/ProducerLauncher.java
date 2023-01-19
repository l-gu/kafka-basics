package org.demo.kafka.producer;

import java.time.Instant;

import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerLauncher {

	public static void main(String[] args) {

		BasicKafkaProducer producer = new BasicKafkaProducer(
				"glider.srvs.cloudkafka.com:9094", 
				"ieimnqtz", // user name   
				"xxxx") ; // user password 
		
		// prepare the record
		String recordValue = "Current time is " + Instant.now().toString();
		System.out.println("Sending message: " + recordValue);
		ProducerRecord<String, String> producerRecord = new ProducerRecord<>("ieimnqtz-aaa", null, recordValue);

		// No authentication at this step (check credentials with SASL mechanism SCRAM-SHA-512) 
		// Authentication only when "send"
		
		try {
			producer.sendRecord(producerRecord); // if authentication failed => SaslAuthenticationException  
		} catch (Exception e) {
			System.out.println("==================== ERROR ================");
			e.printStackTrace();
		} finally {
			// close the producer at the end
			producer.close();
		}
		
	}

}
