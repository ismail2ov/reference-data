package com.sixgroup.referencedata.integration.testcontainers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeType;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.infrastructure.controller.model.EnrichedTradeRDTO;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;
import com.sixgroup.referencedata.integration.utils.KafkaConsumerTestUtilsConfig;
import com.sixgroup.referencedata.integration.utils.TestTopicsConfiguration;

@ActiveProfiles("test")
@Import({TestcontainersConfiguration.class, TestTopicsConfiguration.class, KafkaConsumerTestUtilsConfig.class})
@DirtiesContext
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Disabled
class EnrichedIntegrationTests {

    @Autowired
    TopicsConfiguration topicsConfiguration;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private KafkaTemplate<IsinDataKey, IsinDataValue> isinKafkaTemplate;

    @Autowired
    private KafkaTemplate<TradeKey, TradeValue> tradeKafkaTemplate;

    @Test
    void whenEnrichedTradeExistsThenItReturns() {
        String isin = "ES0B00157734";
        String tradeRef = "296308";

        publishIsinRecord(isin);
        publishTradeRecords(tradeRef, 296399, isin);

        await()
            .atMost(Duration.ofSeconds(20))
            .pollDelay(Duration.ofSeconds(1))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> {
                ResponseEntity<EnrichedTradeRDTO> response = testRestTemplate.getForEntity("/enriched-trades/" + tradeRef, EnrichedTradeRDTO.class);

                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                assertThat(response.getBody()).isNotNull();
                assertThat(response.getBody().getTradeRef()).isEqualTo(tradeRef);
            });
    }

    private void publishIsinRecord(String isin) {
        IsinDataKey isinDataKey = IsinDataKey.newBuilder().setIsin(isin).build();
        IsinDataValue isinDataValue = IsinDataValue.newBuilder().setMaturityDate(LocalDate.of(2025, 12, 10)).setCurrency("EUR").setCfi("FFDPSX")
            .build();

        isinKafkaTemplate.send(topicsConfiguration.getIsin(), isinDataKey, isinDataValue);
    }

    private void publishTradeRecords(String tradeRef, int securityId, String isin) {
        TradeKey tradeKey = TradeKey.newBuilder().setTradeRef(tradeRef).build();
        TradeValue tradeValue = TradeValue.newBuilder()
            .setSecurityId(securityId)
            .setTradeType(TradeType.VISIBLE_ORDER)
            .setIsin(isin)
            .setQuantity(9)
            .setPrice(9999)
            .setTimestamp(Instant.now().toEpochMilli())
            .build();

        tradeKafkaTemplate.send(topicsConfiguration.getTrades(), tradeKey, tradeValue);
    }

}
