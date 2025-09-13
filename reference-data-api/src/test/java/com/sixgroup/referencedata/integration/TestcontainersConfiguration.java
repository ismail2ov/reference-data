package com.sixgroup.referencedata.integration;

import java.util.Map;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    @DynamicPropertySource
    static void registerKafkaProperties(DynamicPropertyRegistry registry) {
        KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
        kafkaContainer.start();

        registry.add("spring.kafka.bootstrap-servers", kafkaContainer::getBootstrapServers);
        registry.add("spring.kafka.streams.application-id", () -> "test-streams-app");
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"));
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
        return buildTopicWith(topicsConfiguration.getIsin());
    }

    @Bean
    NewTopic tradesTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getTrades());
    }

    @Bean
    NewTopic enrichedTradesTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getEnrichedTrades());
    }

    private static @NotNull NewTopic buildTopicWith(String topicName) {
        return TopicBuilder
            .name(topicName)
            .partitions(1)
            .replicas(1)
            .build();
    }
}
