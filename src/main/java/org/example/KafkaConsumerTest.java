package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerTest {
    public static void main(String[] args) {
        // Конфигурация Kafka Consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "kafka-dev.reliab.tech:9093");
        properties.put("group.id", "example-consumer-group");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");

        // Настройки безопасности
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"aisdev\" " +
                        "password=\"koh7Thoh\";");

        // Создание Kafka Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Подписка на топик
        String topic = "1";
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Subscribed to topic: " + topic);

        try {
            while (true) {
                // Получение сообщений
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf(
                            "Received message: key = %s, value = %s, partition = %d, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset()
                    );
                }
            }
        } finally {
            // Закрытие consumer
            consumer.close();
        }
    }
}
