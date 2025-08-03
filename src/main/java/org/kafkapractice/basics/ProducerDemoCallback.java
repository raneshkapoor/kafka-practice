package org.kafkapractice.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

public class ProducerDemoCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallback.class);

    public static void main(String[] args) {

        log.info("Starting Producer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello World!");

        log.info("Sending message : {}", producerRecord.value());

        producer.send(producerRecord, (recordMetadata, e) -> {
            if (Objects.isNull(e)) {
                log.info("Received Metadata :- Partition : {} Timestamp : {}", recordMetadata.partition(), recordMetadata.timestamp());
            } else {
                log.error("Error while sending message. Error : {}", e.getMessage());
            }
        });

        producer.flush();
        producer.close();

        log.info("Message Sent. Producer Closed.");

    }

}
