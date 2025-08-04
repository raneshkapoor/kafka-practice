package org.kafkapractice.basics;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoGracefulShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoGracefulShutdown.class);

    public static void main(String[] args) {

        log.info("Starting Consumer");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", "my-java-consumer");
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of("first_topic"));

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected Shutdown. Calling Consumer Wakeup.");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        try {
            while (true) {
                log.info("Polling Kafka");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                records.forEach(record -> {
                    log.info("Key : {}", record.key());
                    log.info("Value : {}", record.value());
                    log.info("Partition : {}", record.partition());
                });
            }
        } catch (WakeupException e) {
            log.info("Wakeup Exception");
        } catch (Exception e) {
            log.error("Exception", e);
        } finally {
            consumer.close();
            log.info("Consumer Gracefully Closed");
        }

    }

}
