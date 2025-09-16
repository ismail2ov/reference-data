package com.sixgroup.referencedata.infrastructure.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeType;
import com.sixgroup.avro.trade.TradeValue;

class KafkaStreamsConfigAvroTest {

    private static final String SCHEMA_REGISTRY_SCOPE = "mock://test-scope";

    private static TopicsConfiguration topicsConfiguration;
    private static AvroSerdes avroSerdes;

    private TopologyTestDriver topologyTestDriver;

    private TestInputTopic<IsinDataKey, IsinDataValue> isinInputTopic;
    private TestInputTopic<TradeKey, TradeValue> tradeInputTopic;
    private TestOutputTopic<EnrichedTradeKey, EnrichedTradeValue> enrichedOutputTopic;

    @BeforeAll
    static void beforeAll() {
        topicsConfiguration = new TopicsConfiguration();
        topicsConfiguration.setIsin("isin-topic");
        topicsConfiguration.setTrades("trade-topic");
        topicsConfiguration.setEnrichedTrades("enriched-topic");

        avroSerdes = new AvroSerdes(SCHEMA_REGISTRY_SCOPE);
    }

    @BeforeEach
    void setup() {

        KafkaStreamsConfig config = new KafkaStreamsConfig(topicsConfiguration, avroSerdes);

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<IsinDataKey, IsinDataValue> isinTable = config.isinGlobalTable(builder);

        config.tradeEnrichmentStream(builder, isinTable);
        config.enrichedTradeGlobalTable(builder);

        topologyTestDriver = new TopologyTestDriver(builder.build());

        isinInputTopic = topologyTestDriver.createInputTopic(
            "isin-topic",
            avroSerdes.getIsinKeySerde().serializer(),
            avroSerdes.getIsinValueSerde().serializer()
        );
        tradeInputTopic = topologyTestDriver.createInputTopic(
            "trade-topic",
            avroSerdes.getTradeKeySerde().serializer(),
            avroSerdes.getTradeValueSerde().serializer()
        );
        enrichedOutputTopic = topologyTestDriver.createOutputTopic(
            "enriched-topic",
            avroSerdes.getEnrichedTradeKeySerde().deserializer(),
            avroSerdes.getEnrichedTradeValueSerde().deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
        }
    }

    @Test
    void shouldEnrichTradeWhenIsinExists() {
        IsinDataKey isinKey = buildIsinDataKey("ES0B00161819");
        IsinDataValue isinValue = buildIsinDataValue("EUR", "FANAQ5P");
        isinInputTopic.pipeInput(isinKey, isinValue);

        TradeKey tradeKey = buildTradeKey("T12345");
        TradeValue tradeValue = buildTradeValue("ES0B00161819", 123456, 100, 1050);
        tradeInputTopic.pipeInput(tradeKey, tradeValue);

        var queueSize = enrichedOutputTopic.getQueueSize();
        var enrichedTrade = enrichedOutputTopic.readKeyValue();

        assertThat(queueSize).isOne();

        assertThat(enrichedTrade)
            .extracting(
                t -> t.key.getTradeRef().toString(),
                t -> t.value.getCurrency().toString(),
                t -> t.value.getSecurityId()
            )
            .containsExactly("T12345", "EUR", 123456);

    }

    @Test
    void shouldNotEnrichWhenIsinNotExists() {
        TradeKey tradeKey = buildTradeKey("T23456");
        TradeValue tradeValue = buildTradeValue("UNKNOWN", 234567, 50, 2099);

        tradeInputTopic.pipeInput(tradeKey, tradeValue);

        var queueSize = enrichedOutputTopic.getQueueSize();

        assertThat(queueSize).isZero();
    }

    @Test
    void shouldStoreEnrichedTradesInGlobalStore() {
        IsinDataKey isinKey = buildIsinDataKey("ES0B00148006");
        IsinDataValue isinValue = buildIsinDataValue("USD", "FANAU5C");
        isinInputTopic.pipeInput(isinKey, isinValue);

        TradeKey tradeKey = buildTradeKey("T34567");
        TradeValue tradeValue = buildTradeValue("ES0B00148006", 345678, 163, 9999);
        tradeInputTopic.pipeInput(tradeKey, tradeValue);

        enrichedOutputTopic.readKeyValue();

        KeyValueStore<EnrichedTradeKey, EnrichedTradeValue> store = topologyTestDriver.getKeyValueStore(KafkaStreamsConfig.ENRICHED_TRADE_STORE);

        EnrichedTradeValue stored = store.get(
            EnrichedTradeKey.newBuilder().setTradeRef("T34567").build()
        );

        assertThat(stored)
            .extracting(
                s -> s.getCurrency().toString(),
                s -> s.getSecurityId()
            )
            .containsExactly("USD", 345678);
    }

    private IsinDataKey buildIsinDataKey(String isin) {
        return IsinDataKey.newBuilder().setIsin(isin).build();
    }

    private IsinDataValue buildIsinDataValue(String currency, String cfi) {
        return IsinDataValue.newBuilder()
            .setCurrency(currency)
            .setMaturityDate(LocalDate.of(2025, 12, 10))
            .setCfi(cfi)
            .build();
    }

    private TradeKey buildTradeKey(String tradeRef) {
        return TradeKey.newBuilder().setTradeRef(tradeRef).build();
    }

    private TradeValue buildTradeValue(String isin, int securityId, int quantity, int price) {
        return TradeValue.newBuilder()
            .setIsin(isin)
            .setSecurityId(securityId)
            .setTradeType(TradeType.MARKET_AT_CLOSE)
            .setQuantity(quantity)
            .setPrice(price)
            .setTimestamp(System.currentTimeMillis())
            .build();
    }
}
