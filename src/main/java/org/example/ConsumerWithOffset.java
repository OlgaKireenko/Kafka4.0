package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerWithOffset {
    public static void main(String[] args) {
        // Конфигурация для подключения к Kafka
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-dev.reliab.tech:9093");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer-group");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"aisdev\" " +
                        "password=\"koh7Thoh\";");

        // Создаем Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Тема и партиция, которую будем читать
        String topic = "NEW_TOPIC";
        TopicPartition partition = new TopicPartition(topic, 0); // Чтение из партиции 0
        long offset = 13; // Укажите оффсет, с которого нужно начать чтение

        // Подписываемся на конкретную партицию
        consumer.assign(Collections.singletonList(partition));

        // Устанавливаем начальный оффсет
        consumer.seek(partition, offset);

        System.out.println("Чтение сообщений с оффсета " + offset + " из партиции " + partition.partition());

        // Читаем сообщения
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Партия: %d, Оффсет: %d, Ключ: %s, Значение: %s%n",
                            record.partition(), record.offset(), record.key(), record.value());
                }
            }
        } catch (Exception e) {
            System.err.println("Ошибка при чтении сообщений: " + e.getMessage());
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}