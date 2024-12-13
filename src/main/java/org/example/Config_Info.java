package org.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Config_Info {
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
            String topicName = "NEW_TOPIC";
            Map<String, TopicDescription> topicDescriptions = adminClient.describeTopics(Collections.singletonList(topicName)).all().get();

            // Вывод информации о партициях и брокерах
            TopicDescription description = topicDescriptions.get(topicName);
            int replicationFactor = description.partitions().get(0).replicas().size();
            System.out.println("Topic: " + topicName);
            System.out.println("Number of partitions: " + description.partitions().size());
            System.out.println("Replication Factor:"+replicationFactor);
            for (TopicPartitionInfo partitionInfo : description.partitions()) {
                System.out.println("Partition: " + partitionInfo.partition());
                System.out.println("  Leader Broker: " + partitionInfo.leader().id());
                System.out.print("  Replicas: ");
                partitionInfo.replicas().forEach(replica -> System.out.print(replica.id() + " "));
                System.out.println();
                System.out.print("  In-Sync Replicas (ISR): ");
                partitionInfo.isr().forEach(isr -> System.out.print(isr.id() + " "));
                System.out.println("\n");
            }
        } catch (Exception e) {
            System.err.println("Failed to get topic description: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
