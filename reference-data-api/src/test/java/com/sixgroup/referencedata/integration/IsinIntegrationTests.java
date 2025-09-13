package com.sixgroup.referencedata.integration;

import static org.assertj.core.api.Assertions.assertThat;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinRDTO;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

@ActiveProfiles("test")
@Import({TestcontainersConfiguration.class, KafkaConsumerTestUtilsConfig.class})
@DirtiesContext(classMode = AFTER_EACH_TEST_METHOD)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class IsinIntegrationTests {

    public static final String ISIN = "ES0B00157734";

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

        IsinDataKey isinDataKey = IsinDataKey.newBuilder().setIsin(ISIN).build();
        IsinDataValue isinDataValue = IsinDataValue.newBuilder().setMaturityDate(LocalDate.of(2025, 12, 10)).setCurrency("EUR").setCfi("FFDPSX")
            .build();

        IsinRDTO newIsin = new IsinRDTO()
            .isin(ISIN)
            .maturityDate(LocalDate.of(2025, 12, 10))
            .currency("EUR")
            .cfi("FFDPSX");

        kafkaTemplate.send(topicsConfiguration.getIsin(), isinDataKey, isinDataValue);

        ResponseEntity<IsinRDTO> response = testRestTemplate.getForEntity("/isins/" + ISIN, IsinRDTO.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).isEqualTo(newIsin);
    }

}
