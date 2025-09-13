package com.sixgroup.referencedata.infrastructure.messaging.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;

import lombok.RequiredArgsConstructor;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    public static final String ISIN_STORE = "isin-store";

    private final TopicsConfiguration topicsConfiguration;
    private final AvroSerdes avroSerdes;

    @Bean
    public GlobalKTable<IsinDataKey, IsinDataValue> isinGlobalTable(StreamsBuilder streamsBuilder) {
        return streamsBuilder.globalTable(
            topicsConfiguration.getIsin(),
            Consumed.with(avroSerdes.getIsinKeySerde(), avroSerdes.getIsinValueSerde()),
            Materialized.<IsinDataKey, IsinDataValue, KeyValueStore<Bytes, byte[]>>as(ISIN_STORE)
                .withKeySerde(avroSerdes.getIsinKeySerde())
                .withValueSerde(avroSerdes.getIsinValueSerde())
        );
    }
}