package org.demo.kafka.producer;

import java.time.Instant;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BasicKafkaProducer {

	public static void main(String[] args) {

//        //create kafka producer
//        Properties properties = new Properties();
//        // A Kafka producer has three mandatory properties:
//        //	bootstrap.servers
//        //	key.serializer
//        //	value.serializer
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//
//        Producer<String, String> producer = new KafkaProducer<>(properties);

		//Producer<String, String> producer = buildProducer("glider.srvs.cloudkafka.com:9094");
		Producer<String, String> producer = new KafkaProducer<>(KafkaProperties.getProperties(
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

//	private static final String STRING_SERIALIZER = StringSerializer.class.getCanonicalName();
//
//	private static Producer<String, String> buildProducerOLD(String servers) {
//		
//		// See : https://kafka.apache.org/documentation/#producerconfigs 
//		
//		Properties properties = new Properties();
//		// A Kafka producer has three mandatory properties:
//		// bootstrap.servers
//		// key.serializer
//		// value.serializer
//		/*
//		 * "bootstrap.servers" : List of host:port pairs of brokers that the producer
//		 * will use to establish initial connection to the Kafka cluster. This list
//		 * doesn’t need to include all brokers, since the producer will get more
//		 * information after the initial connection. But it is recommended to include at
//		 * least two, so in case one broker goes down, the producer will still be able
//		 * to connect to the cluster.
//		 */
//		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
//
//		/*
//		 * "key.serializer" : Name of a class that will be used to serialize the keys of
//		 * the records we will produce to Kafka. Kafka brokers expect byte arrays as
//		 * keys and values of messages.
//		 */
//		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
//
//		/*
//		 * "value.serializer" : Name of a class that will be used to serialize the
//		 * values of the records we will produce to Kafka.
//		 */
//		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
//
//		Producer<String, String> producer = new KafkaProducer<>(properties);
//		return producer;
//	}
//	
//	private static Producer<String, String> buildProducer(String brokers, String username, String password) {
//		Properties properties = KafkaProperties.getProperties(brokers, username, password);
//		return new KafkaProducer<>(properties);
//	}
}
