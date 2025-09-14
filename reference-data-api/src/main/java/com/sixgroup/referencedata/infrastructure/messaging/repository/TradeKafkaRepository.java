package com.sixgroup.referencedata.infrastructure.messaging.repository;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Repository;

import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.domain.TradeRepository;
import com.sixgroup.referencedata.domain.TradeVO;
import com.sixgroup.referencedata.domain.TradesPageVO;
import com.sixgroup.referencedata.infrastructure.mapper.TradeMapper;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Repository
@RequiredArgsConstructor
@Slf4j
public class TradeKafkaRepository implements TradeRepository {

    private final KafkaTemplate<TradeKey, TradeValue> kafkaTemplate;
    private final TradeConsumer tradeConsumer;
    private final TopicsConfiguration topicsConfiguration;
    private final TradeMapper tradeMapper;

    @Override
    public TradeVO persist(TradeVO tradeVO) {
        TradeKey key = tradeMapper.keyFrom(tradeVO);
        TradeValue value = tradeMapper.valueFrom(tradeVO);
        this.publishTrade(key, value);
        return tradeVO;
    }

    @Override
    public TradesPageVO getTrades(Integer page, Integer size) {
        return tradeConsumer.getTrades(page, size);
    }

    private void publishTrade(TradeKey key, TradeValue value) {
        kafkaTemplate.send(topicsConfiguration.getTrades(), key, value)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.trace("The message with key: {}, and value: {} has been published successfully.", key, value);
                } else {
                    log.error("An error occurred while publishing the message with key: {}, and value: {}.", key, value);
                }
            });
    }
}
