package com.sixgroup.referencedata.infrastructure.messaging.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.enriched.trade.TradeType;
import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;

import lombok.RequiredArgsConstructor;

@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
public class KafkaStreamsConfig {

    public static final String ISIN_STORE = "isin-store";
    public static final String ENRICHED_TRADE_STORE = "enriched-trade-store";

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

    @Bean
    public KStream<EnrichedTradeKey, EnrichedTradeValue> tradeEnrichmentStream(
        StreamsBuilder streamsBuilder,
        GlobalKTable<IsinDataKey, IsinDataValue> isinGlobalTable) {

        KStream<TradeKey, TradeValue> tradeStream = streamsBuilder.stream(
            topicsConfiguration.getTrades(),
            Consumed.with(avroSerdes.getTradeKeySerde(), avroSerdes.getTradeValueSerde())
        );

        KStream<TradeKey, EnrichedTradeValue> enrichedByValue = tradeStream.join(
            isinGlobalTable,
            (tradeKey, tradeValue) -> IsinDataKey.newBuilder().setIsin(tradeValue.getIsin()).build(),
            (tradeValue, isinValue) -> {
                if (isinValue == null) {
                    return null;
                }
                return EnrichedTradeValue.newBuilder()
                    .setSecurityId(tradeValue.getSecurityId())
                    .setTradeType(TradeType.valueOf(tradeValue.getTradeType().name()))
                    .setQuantity(tradeValue.getQuantity())
                    .setPrice(tradeValue.getPrice())
                    .setCurrency(isinValue.getCurrency())
                    .setMaturityDate(isinValue.getMaturityDate())
                    .setCfi(isinValue.getCfi())
                    .setTimestamp(tradeValue.getTimestamp())
                    .build();
            }
        );

        KStream<EnrichedTradeKey, EnrichedTradeValue> enrichedStream = enrichedByValue
            .filter((tradeKey, enrichedValue) -> enrichedValue != null)
            .selectKey((tradeKey, enrichedValue) -> EnrichedTradeKey.newBuilder().setTradeRef(tradeKey.getTradeRef()).build()
            );

        enrichedStream.to(
            topicsConfiguration.getEnrichedTrades(),
            Produced.with(avroSerdes.getEnrichedTradeKeySerde(), avroSerdes.getEnrichedTradeValueSerde())
        );

        return enrichedStream;
    }

    @Bean
    public GlobalKTable<EnrichedTradeKey, EnrichedTradeValue> enrichedTradeGlobalTable(StreamsBuilder streamsBuilder) {
        final KeyValueBytesStoreSupplier store = Stores
            .persistentKeyValueStore(KafkaStreamsConfig.ENRICHED_TRADE_STORE);

        return streamsBuilder.globalTable(
            topicsConfiguration.getEnrichedTrades(),
            Consumed.with(avroSerdes.getEnrichedTradeKeySerde(), avroSerdes.getEnrichedTradeValueSerde()),
            Materialized.as(store)
        );
    }
}