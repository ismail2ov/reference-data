package com.sixgroup.referencedata.integration.embeddedkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeType;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeTypeRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradesListRDTO;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.KafkaStreamsConfig;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;
import com.sixgroup.referencedata.integration.utils.KafkaConsumerTestUtilsConfig;
import com.sixgroup.referencedata.integration.utils.TestTopicsConfiguration;

@ActiveProfiles("test")
@Import({TestTopicsConfiguration.class, KafkaConsumerTestUtilsConfig.class})
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka
@Disabled
class TradeEmbeddedKafkaIntegrationTests {

    public static final String ISIN = "ES0B00152511";

    @Autowired
    TopicsConfiguration topicsConfiguration;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private KafkaTemplate<IsinDataKey, IsinDataValue> isinKafkaTemplate;

    @Autowired
    private KafkaTemplate<TradeKey, TradeValue> tradeKafkaTemplate;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    private static final AtomicInteger counter = new AtomicInteger(1);

    @Test
    void whenCreateNewTradeWithExistingIsinThenEnrichedTradeRecordIsCreated() throws InterruptedException {

        publishIsinRecord(ISIN);

        String tradeRef = "123456";
        Instant now = Instant.now();
        TradeRDTO newTrade = new TradeRDTO()
            .tradeRef(tradeRef)
            .tradeType(TradeTypeRDTO.VISIBLE_ORDER)
            .quantity(1001)
            .price(15203)
            .timestamp(now.toEpochMilli())
            .securityId((int) (Instant.now().toEpochMilli() % Integer.MAX_VALUE))
            .isin(ISIN);

        ResponseEntity<TradeRDTO> response = testRestTemplate.postForEntity("/trades", newTrade, TradeRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        TimeUnit.SECONDS.sleep(1);

        Optional<EnrichedTradeValue> value = getFromStoreByKey(tradeRef);

        assertThat(value).isPresent();
    }

    @Test
    void whenCreateNewTradeWithNonExistingIsinThenEnrichedTradeRecordIsNotCreated() {
        String tradeRef = "987654";
        Instant now = Instant.now();
        TradeRDTO newTrade = new TradeRDTO()
            .tradeRef(tradeRef)
            .tradeType(TradeTypeRDTO.VISIBLE_ORDER)
            .quantity(1001)
            .price(15203)
            .timestamp(now.toEpochMilli())
            .securityId((int) (Instant.now().toEpochMilli() % Integer.MAX_VALUE))
            .isin(ISIN);

        ResponseEntity<TradeRDTO> response = testRestTemplate.postForEntity("/trades", newTrade, TradeRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        Optional<EnrichedTradeValue> value = getFromStoreByKey(tradeRef);

        assertThat(value).isNotPresent();
    }

    @Test
    void whenThereAreFiveDifferentTradesThenItReturnsAListOfIsins() {
        publishIsinRecord(ISIN);

        List<String> tradesRefList = List.of("256310", "256311", "256312", "256313", "256314");

        List<TradeRDTO> expected = publishTradeRecords(tradesRefList, ISIN);

        ResponseEntity<TradesListRDTO> response = testRestTemplate.getForEntity("/trades", TradesListRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getData()).containsAll(expected);
    }

    @Test
    void whenThereIsTradesPageThenReturnsIt() {
        publishIsinRecord(ISIN);

        List<String> tradesRefList = List.of("256310", "256311", "256312", "256313", "256314");

        publishTradeRecords(tradesRefList, ISIN);

        ResponseEntity<TradesListRDTO> response = testRestTemplate.getForEntity("/trades?page=2&size=2", TradesListRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
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

    private List<TradeRDTO> publishTradeRecords(List<String> tradesRefList, String isin) {
        List<TradeRDTO> tradesResponse = new ArrayList<>();

        for (String tradeRef : tradesRefList) {
            TradeKey tradeKey = TradeKey.newBuilder().setTradeRef(tradeRef).build();
            TradeValue tradeValue = TradeValue.newBuilder()
                .setSecurityId(counter.getAndIncrement())
                .setTradeType(TradeType.VISIBLE_ORDER)
                .setIsin(isin)
                .setQuantity(1)
                .setPrice(15203)
                .setTimestamp(Instant.now().toEpochMilli())
                .build();

            tradeKafkaTemplate.send(topicsConfiguration.getTrades(), tradeKey, tradeValue);
            TradeRDTO tradeRDTO = tradeRtoFrom(tradeKey, tradeValue);
            tradesResponse.add(tradeRDTO);
        }

        return tradesResponse;
    }

    private TradeRDTO tradeRtoFrom(TradeKey tradeKey, TradeValue tradeValue) {
        return new TradeRDTO()
            .tradeRef(tradeKey.getTradeRef().toString())
            .tradeType(TradeTypeRDTO.fromValue(tradeValue.getTradeType().name()))
            .quantity(Math.toIntExact(tradeValue.getQuantity()))
            .price(Math.toIntExact(tradeValue.getPrice()))
            .timestamp(tradeValue.getTimestamp())
            .securityId(tradeValue.getSecurityId())
            .isin(tradeValue.getIsin().toString());
    }

}
