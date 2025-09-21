package com.sixgroup.referencedata.integration.embeddedkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinListRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinRDTO;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;
import com.sixgroup.referencedata.integration.utils.KafkaConsumerTestUtils;
import com.sixgroup.referencedata.integration.utils.KafkaConsumerTestUtilsConfig;
import com.sixgroup.referencedata.integration.utils.TestTopicsConfiguration;

@ActiveProfiles("test")
@Import({TestTopicsConfiguration.class, TestTopicsConfiguration.class, KafkaConsumerTestUtilsConfig.class})
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka
class IsinEmbeddedKafkaIntegrationTests {

    @Autowired
    TopicsConfiguration topicsConfiguration;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Autowired
    private KafkaTemplate<IsinDataKey, IsinDataValue> kafkaTemplate;

    @Autowired
    private KafkaConsumerTestUtils<IsinDataKey, IsinDataValue> isinConsumer;

    @Test
    void whenCreateNewIsinThenReturnResourceLocationHeader() {

        String isin = "ES0B00152511";
        IsinRDTO newIsin = new IsinRDTO()
            .isin(isin)
            .maturityDate(LocalDate.of(2025, 12, 10))
            .currency("EUR")
            .cfi("FFDPSX");

        ResponseEntity<Void> response = testRestTemplate.postForEntity("/isins", newIsin, Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(Objects.requireNonNull(response.getHeaders().get("location")).getFirst()).isEqualTo("/isins/" + isin);

        List<ConsumerRecord<IsinDataKey, IsinDataValue>> records = isinConsumer.findAllRecordsByKey(isin, Duration.ofSeconds(10));

        assertThat(records).hasSize(1);
    }

    @Test
    void whenIsinExistsThenReturnIsin() {
        String isin = "ES0B00157734";

        IsinRDTO newIsin = publishIsinRecord(isin);

        ResponseEntity<IsinRDTO> response = testRestTemplate.getForEntity("/isins/" + isin, IsinRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isEqualTo(newIsin);
    }

    @Test
    void whenThereAreFourDifferentIsinsThenItReturnsAListOfIsins() {
        List<IsinRDTO> expected = List.of(
            publishIsinRecord("ES0B00152511"),
            publishIsinRecord("ES0B00157734"),
            publishIsinRecord("ES0B00162973"),
            publishIsinRecord("ES0B00168079")
        );
        publishIsinRecord("ES0B00152511");

        ResponseEntity<IsinListRDTO> response = testRestTemplate.getForEntity("/isins", IsinListRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody().getData()).containsAnyElementsOf(expected);
    }

    @Test
    void whenThereIsIsinPageThenReturnsIt() {
        publishIsinRecord("ES0B00165083");
        publishIsinRecord("ES0B00164946");
        publishIsinRecord("ES0B00165067");
        publishIsinRecord("ES0B00164920");
        publishIsinRecord("ES0B00166289");

        await()
            .atMost(Duration.ofSeconds(20))
            .pollInterval(Duration.ofSeconds(1))
            .untilAsserted(() -> {
                ResponseEntity<IsinListRDTO> response = testRestTemplate.getForEntity("/isins?page=2&size=2", IsinListRDTO.class);

                assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                assertThat(response.getBody()).isNotNull();
                assertThat(response.getBody().getData()).hasSize(2);
            });
    }

    private IsinRDTO publishIsinRecord(String isin) {
        IsinDataKey isinDataKey = IsinDataKey.newBuilder().setIsin(isin).build();
        IsinDataValue isinDataValue = IsinDataValue.newBuilder().setMaturityDate(LocalDate.of(2025, 12, 10)).setCurrency("EUR").setCfi("FFDPSX")
            .build();

        IsinRDTO newIsin = new IsinRDTO()
            .isin(isin)
            .maturityDate(LocalDate.of(2025, 12, 10))
            .currency("EUR")
            .cfi("FFDPSX");

        kafkaTemplate.send(topicsConfiguration.getIsin(), isinDataKey, isinDataValue);
        return newIsin;
    }

}
