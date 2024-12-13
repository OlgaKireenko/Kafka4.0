package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerTest {

    public static void main(String[] args) {
        // Конфигурация для подключения к Kafka
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-dev.reliab.tech:9093"); // URL Kafka
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("security.protocol", "SASL_PLAINTEXT");  // Протокол безопасности
        properties.put("sasl.mechanism", "PLAIN");              // Механизм аутентификации
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"aisdev\" password=\"koh7Thoh\";");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "java-producer");

        // Создаем объект Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        try {
            // Создаем сообщение для отправки в Kafka
            String topic = "NEW_TOPIC";  // Тема, в которую отправляем сообщение
            String message = "Test Message7______*****"; // Текст сообщения

            // Создаем и отправляем сообщение
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, message);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("Ошибка при отправке сообщения: " + exception.getMessage());
                } else {
                    System.out.println("Сообщение отправлено в топик " + topic + " с offset " + metadata.offset());
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Закрываем продюсера
            producer.close();
        }
    }
}