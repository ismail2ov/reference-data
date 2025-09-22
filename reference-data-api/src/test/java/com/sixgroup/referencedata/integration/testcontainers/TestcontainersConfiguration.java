package com.sixgroup.referencedata.integration.testcontainers;

import java.time.Duration;
import java.util.Map;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    public static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("apache/kafka-native:latest"))
        .withStartupTimeout(Duration.ofMinutes(2))
        .withStartupAttempts(3)
        .withReuse(false);

    static {
        System.setProperty("testcontainers.reuse.enable", "false");
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

}
