package com.sixgroup.referencedata.integration.utils;

import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

import org.apache.kafka.clients.admin.NewTopic;
import org.jetbrains.annotations.NotNull;

import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

public class TestTopicsConfiguration {

    @Bean
    NewTopic isinTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getIsin(), 1);
    }

    @Bean
    NewTopic tradesTopic(TopicsConfiguration topicsConfiguration) {
        return buildTopicWith(topicsConfiguration.getTrades(), 1);
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
