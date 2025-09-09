package com.sixgroup.referencedata.integration;

import static org.assertj.core.api.Assertions.assertThat;

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
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.infrastructure.configuration.TopicsConfiguration;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinRDTO;

@ActiveProfiles("test")
@Import(TestcontainersConfiguration.class)
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
    private ConsumerFactory<IsinDataKey, IsinDataValue> consumerFactory;

    @Test
    void whenCreateNewIsinThenReturnResourceLocationHeader() {
        Consumer<IsinDataKey, IsinDataValue> testConsumer = consumerFactory.createConsumer("test-consumer-group-id", "test");
        testConsumer.subscribe(List.of(topicsConfiguration.getIsin()));

        IsinRDTO newIsin = new IsinRDTO()
            .isin(ISIN)
            .maturityDate(LocalDate.of(2025, 12, 10))
            .currency("EUR")
            .cfi("FFDPSX");

        ResponseEntity<Void> response = testRestTemplate.postForEntity("/isins", newIsin, Void.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(Objects.requireNonNull(response.getHeaders().get("location")).getFirst()).isEqualTo("/isins/ES0B00157734");

        ConsumerRecord<IsinDataKey, IsinDataValue> received = KafkaTestUtils.getSingleRecord(testConsumer, topicsConfiguration.getIsin(), Duration
            .ofSeconds(30L));

        assertThat(received.toString()).contains(ISIN);
    }

}
