package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class KafkaTopicInfo {
    public static void main(String[] args) {
        // Конфигурация для подключения к Kafka
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-dev.reliab.tech:9093");
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"aisdev\" " +
                        "password=\"koh7Thoh\";");

        // Создаем AdminClient
        try (AdminClient adminClient = AdminClient.create(properties)) {
            // Получение информации о топике
            String topicName = "Test_topic";
            Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all().get();

            // Вывод информации о партициях
            TopicDescription description = topicDescriptions.get(topicName);
            int replicationFactor = description.partitions().get(0).replicas().size();
            System.out.println("Topic: " + topicName);
            System.out.println("Number of partitions: " + description.partitions().size());
            System.out.println("Replication factor:" + replicationFactor);

        } catch (Exception e) {
            System.err.println("Failed to get topic description: " + e.getMessage());
            e.printStackTrace();
        }
    }
}