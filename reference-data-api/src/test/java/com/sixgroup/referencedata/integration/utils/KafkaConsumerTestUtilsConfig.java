package com.sixgroup.referencedata.integration.utils;

import java.util.List;
import java.util.UUID;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.DependsOn;
import org.springframework.kafka.core.ConsumerFactory;

import org.apache.kafka.clients.consumer.Consumer;
import org.jetbrains.annotations.NotNull;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

@TestConfiguration
public class KafkaConsumerTestUtilsConfig {

    @Bean(destroyMethod = "close")
    @DependsOn("isinTopic")
    public KafkaConsumerTestUtils<IsinDataKey, IsinDataValue> isinConsumer(
        ConsumerFactory<IsinDataKey, IsinDataValue> consumerFactory,
        TopicsConfiguration topicsConfiguration
    ) {
        Consumer<IsinDataKey, IsinDataValue> consumer = createConsumer(consumerFactory);

        return new KafkaConsumerTestUtils<>(consumer, List.of(topicsConfiguration.getIsin()));
    }

    @Bean(destroyMethod = "close")
    @DependsOn("tradesTopic")
    public KafkaConsumerTestUtils<TradeKey, TradeValue> tradesConsumer(
        ConsumerFactory<TradeKey, TradeValue> consumerFactory,
        TopicsConfiguration topicsConfiguration
    ) {
        Consumer<TradeKey, TradeValue> consumer = createConsumer(consumerFactory);

        return new KafkaConsumerTestUtils<>(consumer, List.of(topicsConfiguration.getTrades()));
    }

    @Bean(destroyMethod = "close")
    @DependsOn("enrichedTradesTopic")
    public KafkaConsumerTestUtils<EnrichedTradeKey, EnrichedTradeValue> enrichedTradesConsumer(
        ConsumerFactory<EnrichedTradeKey, EnrichedTradeValue> consumerFactory,
        TopicsConfiguration topicsConfiguration
    ) {
        Consumer<EnrichedTradeKey, EnrichedTradeValue> consumer = createConsumer(consumerFactory);

        return new KafkaConsumerTestUtils<>(consumer, List.of(topicsConfiguration.getEnrichedTrades()));
    }

    private static <K, V> @NotNull Consumer<K, V> createConsumer(ConsumerFactory<K, V> consumerFactory) {
        return consumerFactory.createConsumer(
            "test-group-" + UUID.randomUUID(),
            "test"
        );
    }

}
