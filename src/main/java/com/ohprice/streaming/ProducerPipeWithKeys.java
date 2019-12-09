package com.ohprice.streaming;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerPipeWithKeys {

    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerPipeWithKeys.class);

        System.out.println("Running ProducePipe");
        String bootstrapServers = "127.0.0.1:9092";

        //Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Create Producer record
        for (int i = 0; i < 10; i++) {

            String topic = "movie_topic";
            String value = "Slow Country __" + i;
            String key = "id_+ " + i;

            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            logger.info("Key: " + key);
            //Send Data - Asynchronous

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        //record successfully sent
                        logger.info("Received new metadata. \n " +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                        recordMetadata.toString();
                    } else {
                        logger.error("__Error__ while producing: ", e);
                    }
                }
            });
        }

//        flush data
        producer.flush();

//        flush and close
        producer.close();


    }
}
