package io.conduktor.demos.kafka;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        // create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record
        ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>("demo_java", "hello world");

        // send the data - asynchronous
        /*
        For performing the callbacks, the user needs to implement a callback function. This function is implemented for
        asynchronously handling the request completion. That's why it's return type will be void. This function will be
        implemented in the block where the producer sends data to the Kafka. There is no requirement to make changes in
        other blocks of codes.
        The callback function used by the producer is the onCompletion(). Basically, this method requires two arguments:
        1. Metadata of the record  2. exception
         */
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                //executes everytime a record is sent or an exception is thrown
                if ( e == null){
                    log.info("recieved new metadata" + "\n" +
                        "Topic : " + metadata.topic() + "\n" +
                         " partition : " + metadata.partition() + "\n" +
                         " offsets : " + metadata.offset() + "\n" +
                         " timestamp : " + metadata.timestamp() + "\n");

                }else{
                    log.error("error while producing" , e);
                }

            }
        });

        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
