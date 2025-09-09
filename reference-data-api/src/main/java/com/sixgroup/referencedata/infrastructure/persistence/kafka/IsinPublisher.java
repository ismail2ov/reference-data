package com.sixgroup.referencedata.infrastructure.persistence.kafka;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.infrastructure.configuration.TopicsConfiguration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@RequiredArgsConstructor
@Slf4j
public class IsinPublisher {

    private final KafkaTemplate<IsinDataKey, IsinDataValue> kafkaTemplate;
    private final TopicsConfiguration topicsConfiguration;

    public void publishTrade(IsinDataKey key, IsinDataValue value) {
        kafkaTemplate.send(topicsConfiguration.getIsin(), key, value)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.trace("The message with key: {}, and value: {} has been published successfully.", key, value);
                } else {
                    log.error("An error occurred while publishing the message with key: {}, and value: {}.", key, value);
                }
            });
    }
}