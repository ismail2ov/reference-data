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

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;

class IsinKTableTopologyTest {

    private static final String SCHEMA_REGISTRY_SCOPE = "mock://test-scope";
    public static final String ISIN_1 = "ES0B00168228";
    public static final String ISIN_2 = "ES0B00168376";

    private static TopicsConfiguration topicsConfiguration;
    private static AvroSerdes avroSerdes;

    private TopologyTestDriver topologyTestDriver;

    private TestInputTopic<IsinDataKey, IsinDataValue> inputTopic;
    private KeyValueStore<IsinDataKey, IsinDataValue> isinStore;

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
        StreamsBuilder builder = new StreamsBuilder();

        KafkaStreamsConfig kafkaConfig = new KafkaStreamsConfig(topicsConfiguration, avroSerdes);

        kafkaConfig.isinGlobalTable(builder);

        topologyTestDriver = new TopologyTestDriver(builder.build());

        inputTopic = topologyTestDriver.createInputTopic(
            topicsConfiguration.getIsin(),
            avroSerdes.getIsinKeySerde().serializer(),
            avroSerdes.getIsinValueSerde().serializer()
        );

        try {
            isinStore = topologyTestDriver.getKeyValueStore(KafkaStreamsConfig.ISIN_STORE);
        } catch (Exception e) {
            System.out.println("Stores disponibles: " + topologyTestDriver.getAllStateStores().keySet());
            throw new RuntimeException("No se pudo obtener el store: " + KafkaStreamsConfig.ISIN_STORE, e);
        }
    }

    @AfterEach
    void teardown() {
        if (topologyTestDriver != null) {
            topologyTestDriver.close();
        }
    }

    @Test
    void shouldStoreIsinRecordsInGlobalTable() {
        IsinDataKey isinKey1 = buildIsinDataKey(ISIN_1);
        IsinDataValue isinValue1 = buildIsinDataValue("EUR", "FANAQ5P");

        IsinDataKey isinKey2 = buildIsinDataKey(ISIN_2);
        IsinDataValue isinValue2 = buildIsinDataValue("USD", "FANAU5C");

        inputTopic.pipeInput(isinKey1, isinValue1);
        inputTopic.pipeInput(isinKey2, isinValue2);

        assertThat(isinStore.get(isinKey1)).isEqualTo(isinValue1);
        assertThat(isinStore.get(isinKey2)).isEqualTo(isinValue2);
    }

    @Test
    void shouldUpdateIsinInGlobalTableIfKeyExists() {
        IsinDataKey isinKey = buildIsinDataKey(ISIN_1);
        IsinDataValue initial = buildIsinDataValue("EUR", "FMPCMAUG25");
        IsinDataValue updated = buildIsinDataValue("USD", "FTPCW38B25");

        inputTopic.pipeInput(isinKey, initial);
        assertThat(isinStore.get(isinKey)).isEqualTo(initial);

        inputTopic.pipeInput(isinKey, updated);

        assertThat(isinStore.get(isinKey)).isEqualTo(updated);
    }

    @Test
    void shouldReturnNullForNonExistingKeyInGlobalTable() {
        IsinDataKey isinKey = buildIsinDataKey("NON_EXISTENT");

        IsinDataValue isinDataValue = isinStore.get(isinKey);

        assertThat(isinDataValue).isNull();
    }

    @Test
    void shouldHandleTombstoneRecordsInGlobalTable() {
        IsinDataKey isinKey = buildIsinDataKey(ISIN_1);
        IsinDataValue isinValue = buildIsinDataValue("EUR", "FANAQ5P");

        inputTopic.pipeInput(isinKey, isinValue);
        assertThat(isinStore.get(isinKey)).isEqualTo(isinValue);

        inputTopic.pipeInput(isinKey, null);

        assertThat(isinStore.get(isinKey)).isNull();
    }

    @Test
    void shouldIterateOverAllRecordsInGlobalTable() {
        IsinDataKey isinKey1 = buildIsinDataKey(ISIN_1);
        IsinDataValue isinValue1 = buildIsinDataValue("EUR", "FANAQ5P");

        IsinDataKey isinKey2 = buildIsinDataKey(ISIN_2);
        IsinDataValue isinValue2 = buildIsinDataValue("USD", "FANAU5C");

        inputTopic.pipeInput(isinKey1, isinValue1);
        inputTopic.pipeInput(isinKey2, isinValue2);

        Map<IsinDataKey, IsinDataValue> allRecords = new HashMap<>();
        try (KeyValueIterator<IsinDataKey, IsinDataValue> iterator = isinStore.all()) {
            while (iterator.hasNext()) {
                KeyValue<IsinDataKey, IsinDataValue> next = iterator.next();
                allRecords.put(next.key, next.value);
            }
        }

        assertThat(allRecords)
            .hasSize(2)
            .containsEntry(isinKey1, isinValue1)
            .containsEntry(isinKey2, isinValue2);
    }

    @Test
    void shouldHandleMultipleUpdatesCorrectly() {
        IsinDataKey isinKey = buildIsinDataKey(ISIN_1);

        for (int i = 0; i < 5; i++) {
            IsinDataValue value = buildIsinDataValue("EUR", "CFI_" + i);
            inputTopic.pipeInput(isinKey, value);

            assertThat(isinStore.get(isinKey)).isEqualTo(value);
        }
    }

    private IsinDataKey buildIsinDataKey(String isin) {
        return IsinDataKey.newBuilder()
            .setIsin(isin)
            .build();
    }

    private IsinDataValue buildIsinDataValue(String currency, String cfi) {
        return IsinDataValue.newBuilder()
            .setCurrency(currency)
            .setMaturityDate(LocalDate.of(2025, 12, 10))
            .setCfi(cfi)
            .build();
    }
}