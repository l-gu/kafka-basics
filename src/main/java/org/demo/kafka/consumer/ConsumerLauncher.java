package org.demo.kafka.consumer;

import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public class ConsumerLauncher {

	public static void main(String[] args) {

		// TODO : just use KafkaConsumerBuilder returning the standard "Consumer"
		BasicKafkaConsumer consumer = new BasicKafkaConsumer(
				"glider.srvs.cloudkafka.com:9094", 
				"ieimnqtz", // user name   
				"ZuOUQ0mCIhJSRggByPkPcCOpwKrBr77_", // user password 
				10) ; 
		
        // subscribe consumer to our topic(s)
        consumer.subscribe("ieimnqtz-aaa");
        
		while (true) {
            System.out.println("Consumer : POLL");
            ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(Duration.ofMillis(1000)); // If duration too short => Exception
            
            System.out.println("Consumer : " + consumerRecords.count() + " record(s) received");
            if ( stop(consumerRecords.count()) ) {
            	break;
            }
            
            /*
             * Notice if you receive records (consumerRecords.count()!=0), then runConsumer method calls consumer.commitAsync() 
             * which commit offsets returned on the last call to consumer.poll(…) for all the subscribed list of topic partitions.
             */
            consumer.commitAsync();

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer : Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            
		}
		
		/*
		 * Always close() the consumer before exiting. This will close the network connections and sockets. 
		 * It will also trigger a rebalance immediately rather than wait for the group coordinator to discover 
		 * that the consumer stopped sending heartbeats and is likely dead, which will take longer 
		 * and therefore result in a longer period of time in which consumers can't consume messages from a subset of the partitions.
		 */
        System.out.println("Consumer : CLOSE");
		consumer.close();
	    System.out.println("DONE");		
	}

	private static int numberOfEmptyResponses = 0 ;
	private static int maxNumberOfEmptyResponses = 2 ;
	public static boolean stop(int numberOfRecordsReceived) {
		if ( numberOfRecordsReceived == 0 ) {
			numberOfEmptyResponses++ ;
			if ( numberOfEmptyResponses >= maxNumberOfEmptyResponses ) {
				return true;
			}
		}
		else {
			numberOfEmptyResponses = 0; // Reset
		}
		return false;
	}
}
