package com.sixgroup.referencedata.infrastructure.messaging.kafka;

import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import org.apache.avro.specific.SpecificRecord;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import lombok.Getter;

@Component
@Getter
public final class AvroSerdes {

    private final Map<String, Object> serdeConfig;

    private final SpecificAvroSerde<IsinDataKey> isinKeySerde;
    private final SpecificAvroSerde<IsinDataValue> isinValueSerde;

    private final SpecificAvroSerde<TradeKey> tradeKeySerde;
    private final SpecificAvroSerde<TradeValue> tradeValueSerde;

    private final SpecificAvroSerde<EnrichedTradeKey> enrichedTradeKeySerde;
    private final SpecificAvroSerde<EnrichedTradeValue> enrichedTradeValueSerde;

    public AvroSerdes(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        this.serdeConfig = Map.of(
            "schema.registry.url", schemaRegistryUrl,
            "specific.avro.reader", true
        );

        this.isinKeySerde = buildAvroSerde(true);
        this.isinValueSerde = buildAvroSerde(false);

        this.tradeKeySerde = buildAvroSerde(true);
        this.tradeValueSerde = buildAvroSerde(false);

        this.enrichedTradeKeySerde = buildAvroSerde(true);
        this.enrichedTradeValueSerde = buildAvroSerde(false);
    }

    private <T extends SpecificRecord> SpecificAvroSerde<T> buildAvroSerde(boolean isKey) {
        final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(serdeConfig, isKey);
        return serde;
    }

}
