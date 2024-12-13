package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class CreateTestTopic {
    public static void main(String[] args) {
        // Конфигурация Kafka Admin Client
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
            // Создаем новый топик
            String topicName = "test_topic";
            int partitions = 3;
            short replicationFactor = 3;

            NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor)
                    .configs(Map.of(
                            "retention.ms", "604800000",
                            "retention.bytes", "10737418240",
                            "min.insync.replicas", "2",
                            "cleanup.policy", "delete"
                    ));

            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
            System.out.println("Топик успешно создан: " + topicName);
        } catch (Exception e) {
            System.err.println("Ошибка при создании топика: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
