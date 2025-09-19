package com.sixgroup.referencedata.infrastructure.messaging.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.enriched.trade.TradeType;

class EnrichedTradeKTableTopologyTest {

    private static final String SCHEMA_REGISTRY_SCOPE = "mock://test-scope";

    private static TopicsConfiguration topicsConfiguration;
    private static AvroSerdes avroSerdes;

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<EnrichedTradeKey, EnrichedTradeValue> inputTopic;
    private KeyValueStore<EnrichedTradeKey, EnrichedTradeValue> enrichedTradeStore;

    @BeforeAll
    static void beforeAll() {
        topicsConfiguration = new TopicsConfiguration();
        topicsConfiguration.setEnrichedTrades("enriched-trades-topic");
        topicsConfiguration.setIsin("isin-topic");
        topicsConfiguration.setTrades("trades-topic");

        avroSerdes = new AvroSerdes(SCHEMA_REGISTRY_SCOPE);
    }

    @BeforeEach
    void setup() {
        StreamsBuilder builder = new StreamsBuilder();

        KafkaStreamsConfig kafkaConfig = new KafkaStreamsConfig(topicsConfiguration, avroSerdes);

        kafkaConfig.enrichedTradeGlobalTable(builder);

        topologyTestDriver = new TopologyTestDriver(builder.build());

        inputTopic = topologyTestDriver.createInputTopic(
            topicsConfiguration.getEnrichedTrades(),
            avroSerdes.getEnrichedTradeKeySerde().serializer(),
            avroSerdes.getEnrichedTradeValueSerde().serializer()
        );

        enrichedTradeStore = topologyTestDriver.getKeyValueStore(KafkaStreamsConfig.ENRICHED_TRADE_STORE);
    }

    @AfterEach
    void teardown() {
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
        }
    }

    @Test
    void shouldStoreEnrichedTradeRecordsInGlobalTable() {
        EnrichedTradeKey enrichedTradeKey1 = buildTradeKey("TRADE_1");
        EnrichedTradeValue enrichedTradeValue1 = buildTradeValue(8001, 1800, 2999, "EUR", "FANAH6P");

        EnrichedTradeKey enrichedTradeKey2 = buildTradeKey("TRADE_2");
        EnrichedTradeValue enrichedTradeValue2 = buildTradeValue(8002, 2200, 3299, "USD", "FANAM6C");

        inputTopic.pipeInput(enrichedTradeKey1, enrichedTradeValue1);
        inputTopic.pipeInput(enrichedTradeKey2, enrichedTradeValue2);

        assertThat(enrichedTradeStore.get(enrichedTradeKey1)).isEqualTo(enrichedTradeValue1);
        assertThat(enrichedTradeStore.get(enrichedTradeKey2)).isEqualTo(enrichedTradeValue2);
    }

    @Test
    void shouldUpdateTradeInGlobalTableIfKeyExists() {
        EnrichedTradeKey key = buildTradeKey("TRADE_3");
        EnrichedTradeValue initial = buildTradeValue(8003, 1500, 1999, "EUR", "FANAH6P");
        EnrichedTradeValue updated = buildTradeValue(8002, 1800, 1299, "USD", "FANAM6P");

        inputTopic.pipeInput(key, initial);
        assertThat(enrichedTradeStore.get(key)).isEqualTo(initial);

        inputTopic.pipeInput(key, updated);
        assertThat(enrichedTradeStore.get(key)).isEqualTo(updated);
    }

    @Test
    void shouldReturnNullForNonExistingKeyInGlobalTable() {
        EnrichedTradeKey key = buildTradeKey("NON_EXISTENT");

        EnrichedTradeValue value = enrichedTradeStore.get(key);

        assertThat(value).isNull();
    }

    @Test
    void shouldHandleTombstoneRecordsInGlobalTable() {
        EnrichedTradeKey key = buildTradeKey("TRADE_4");
        EnrichedTradeValue value = buildTradeValue(8004, 999, 99, "EUR", "FANAU6C");

        inputTopic.pipeInput(key, value);
        assertThat(enrichedTradeStore.get(key)).isEqualTo(value);

        // Tombstone
        inputTopic.pipeInput(key, null);
        assertThat(enrichedTradeStore.get(key)).isNull();
    }

    @Test
    void shouldIterateOverAllRecordsInGlobalTable() {
        EnrichedTradeKey enrichedTradeKey1 = buildTradeKey("TRADE_5");
        EnrichedTradeValue enrichedTradeValue1 = buildTradeValue(8005, 500, 199, "EUR", "FANAU6P");

        EnrichedTradeKey enrichedTradeKey2 = buildTradeKey("TRADE_6");
        EnrichedTradeValue enrichedTradeValue2 = buildTradeValue(8006, 700, 202, "USD", "FANAZ6P");

        inputTopic.pipeInput(enrichedTradeKey1, enrichedTradeValue1);
        inputTopic.pipeInput(enrichedTradeKey2, enrichedTradeValue2);

        Map<EnrichedTradeKey, EnrichedTradeValue> allRecords = new HashMap<>();
        try (KeyValueIterator<EnrichedTradeKey, EnrichedTradeValue> iterator = enrichedTradeStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<EnrichedTradeKey, EnrichedTradeValue> next = iterator.next();
                allRecords.put(next.key, next.value);
            }
        }

        assertThat(allRecords)
            .hasSize(2)
            .containsEntry(enrichedTradeKey1, enrichedTradeValue1)
            .containsEntry(enrichedTradeKey2, enrichedTradeValue2);
    }

    private EnrichedTradeKey buildTradeKey(String tradeRef) {
        return EnrichedTradeKey.newBuilder()
            .setTradeRef(tradeRef)
            .build();
    }

    private EnrichedTradeValue buildTradeValue(int securityId, int quantity, int price, String currency, String cfi) {
        return EnrichedTradeValue.newBuilder()
            .setSecurityId(securityId)
            .setTradeType(TradeType.MARKET_AT_CLOSE)
            .setQuantity(quantity)
            .setPrice(price)
            .setCurrency(currency)
            .setMaturityDate(LocalDate.of(2025, 9, 19))
            .setCfi(cfi)
            .setTimestamp(System.currentTimeMillis())
            .build();
    }
}
