package com.sixgroup.referencedata.integration.utils;

import java.util.Map;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    public static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));

    static {
        kafkaContainer.start();
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return kafkaContainer;
    }

    @Bean
    KafkaAdmin kafkaAdmin(KafkaContainer kafkaContainer) {
        Map<String, Object> configs = Map.of(
            AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers()
        );
        return new KafkaAdmin(configs);
    }

    @Bean
    NewTopic isinTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getIsin(), 1);
    }

    @Bean
    NewTopic tradesTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getTrades(), 3);
    }

    @Bean
    NewTopic enrichedTradesTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getEnrichedTrades(), 1);
    }

    private static @NotNull NewTopic buildTopicWith(String topicName, int partitionCount) {
        return TopicBuilder
            .name(topicName)
            .partitions(partitionCount)
            .replicas(1)
            .build();
    }
}
