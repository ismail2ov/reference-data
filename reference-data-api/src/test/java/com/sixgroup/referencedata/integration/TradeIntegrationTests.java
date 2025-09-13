package com.sixgroup.referencedata.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeTypeRDTO;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.KafkaStreamsConfig;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

@ActiveProfiles("test")
@Import({TestcontainersConfiguration.class, KafkaConsumerTestUtilsConfig.class})
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class TradeIntegrationTests {

    @Autowired
    TopicsConfiguration topicsConfiguration;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private KafkaTemplate<IsinDataKey, IsinDataValue> isinKafkaTemplate;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @Test
    void whenCreateNewTradeWithExistingIsinThenEnrichedTradeRecordIsCreated() {
        String isin = "ES0B00152511";

        publishIsinRecord(isin);

        String tradeRef = "123456";
        Instant now = Instant.now();
        TradeRDTO newTrade = new TradeRDTO()
            .tradeRef(tradeRef)
            .tradeType(TradeTypeRDTO.VISIBLE_ORDER)
            .quantity(1001)
            .price(15203)
            .timestamp(now.toEpochMilli())
            .securityId((int) (Instant.now().toEpochMilli() % Integer.MAX_VALUE))
            .isin(isin);

        ResponseEntity<TradeRDTO> response = testRestTemplate.postForEntity("/trades", newTrade, TradeRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        Optional<EnrichedTradeValue> value = getFromStoreByKey(tradeRef);

        assertThat(value).isPresent();
    }

    @Test
    void whenCreateNewTradeWithNonExistingIsinThenEnrichedTradeRecordIsNotCreated() {
        String isin = "ES0B00168210";

        String tradeRef = "987654";
        Instant now = Instant.now();
        TradeRDTO newTrade = new TradeRDTO()
            .tradeRef(tradeRef)
            .tradeType(TradeTypeRDTO.VISIBLE_ORDER)
            .quantity(1001)
            .price(15203)
            .timestamp(now.toEpochMilli())
            .securityId((int) (Instant.now().toEpochMilli() % Integer.MAX_VALUE))
            .isin(isin);

        ResponseEntity<TradeRDTO> response = testRestTemplate.postForEntity("/trades", newTrade, TradeRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        Optional<EnrichedTradeValue> value = getFromStoreByKey(tradeRef);

        assertThat(value).isNotPresent();
    }

    private void publishIsinRecord(String isin) {
        IsinDataKey isinDataKey = IsinDataKey.newBuilder().setIsin(isin).build();
        IsinDataValue isinDataValue = IsinDataValue.newBuilder().setMaturityDate(LocalDate.of(2025, 12, 10)).setCurrency("EUR").setCfi("FFDPSX")
            .build();

        isinKafkaTemplate.send(topicsConfiguration.getIsin(), isinDataKey, isinDataValue);
    }

    private Optional<EnrichedTradeValue> getFromStoreByKey(String tradeRef) {
        ReadOnlyKeyValueStore<EnrichedTradeKey, EnrichedTradeValue> keyValueStore = Objects.requireNonNull(
            streamsBuilderFactoryBean.getKafkaStreams())
            .store(StoreQueryParameters.fromNameAndType(
                KafkaStreamsConfig.ENRICHED_TRADE_STORE,
                QueryableStoreTypes.keyValueStore()
            ));

        EnrichedTradeValue value = keyValueStore.get(EnrichedTradeKey.newBuilder().setTradeRef(tradeRef).build());

        return Optional.ofNullable(value);
    }

}
