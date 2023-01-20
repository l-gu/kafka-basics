package org.demo.kafka.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * 
 * see example here : https://github.com/CloudKarafka/java-kafka-example/blob/main/src/main/java/KafkaExample.java 
 * see also : https://fullstackcode.dev/2022/05/30/how-to-publish-subscribe-to-kafka-with-spring-boot-and-sasl-scram/ 
 * 
 * @author L. Guerin
 *
 */
public class KafkaConfig {

	public static Properties getCommonProperties(String brokers, String username, String password) {
		
		Properties props = new Properties();
		
		/*
		 * "bootstrap.servers" : List of host:port pairs of brokers that the producer
		 * will use to establish initial connection to the Kafka cluster. This list
		 * doesn’t need to include all brokers, since the producer will get more
		 * information after the initial connection. But it is recommended to include at
		 * least two, so in case one broker goes down, the producer will still be able
		 * to connect to the cluster.
		 */		
        props.put("bootstrap.servers", brokers);
        
        
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");

        props.put("enable.idempotence" , "false"); // added to avoid ClusterAuthorizationException (errorCode=31)
        
        props.put("security.protocol", "SASL_SSL");
        //props.put("sasl.mechanism", "SCRAM-SHA-256"); 
        props.put("sasl.mechanism", "SCRAM-SHA-512"); // SASL/SCRAM-SHA-512
        
        String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
        String jaasCfg = String.format(jaasTemplate, username, password);
        props.put("sasl.jaas.config", jaasCfg);
        
        return props;
	}
	
	public static Properties getProducerProperties(String brokers, String username, String password) {
		
		Properties props = getCommonProperties(brokers, username, password); 
        
		String serializer = StringSerializer.class.getName();
		/*
		 * "key.serializer" (for Producer) : Name of a class that will be used to serialize the keys of
		 * the records we will produce to Kafka. Kafka brokers expect byte arrays as
		 * keys and values of messages.
		 */
        props.put("key.serializer", serializer);

        /*
		 * "value.serializer" (for Producer) : Name of a class that will be used to serialize the
		 * values of the records we will produce to Kafka.
		 */
        props.put("value.serializer", serializer);
        
        return props;
	}

	public static Properties getConsumerProperties(String brokers, String username, String password, int maxPollRecords) {
		
		/*
		 * For consumer-configuration, see : 
		 * https://www.conduktor.io/kafka/kafka-consumer-important-settings-poll-and-internal-threads-behavior
		 * https://docs.confluent.io/platform/current/clients/consumer.html#ak-consumer-configuration
		 */
		Properties props = getCommonProperties(brokers, username, password); 
        
        /*
         * The group.id is a string that uniquely identifies the group of consumer processes to which this consumer belongs.
         */
        props.put("group.id", username + "-consumer");

		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		
		String deserializer = StringDeserializer.class.getName();
        props.put("key.deserializer", deserializer);
        props.put("value.deserializer", deserializer);
        
        return props;
	}
}
