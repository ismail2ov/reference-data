package com.sixgroup.referencedata.infrastructure.messaging.repository.trade;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Repository;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.domain.trade.TradeVO;
import com.sixgroup.referencedata.domain.trade.TradesPageVO;
import com.sixgroup.referencedata.infrastructure.mapper.TradeMapper;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.TopicsConfiguration;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class TradeConsumer {

    private final KafkaProperties kafkaProperties;
    private final TopicsConfiguration topicsConfiguration;
    private final TradeMapper tradeMapper;

    public TradesPageVO getTrades(Integer page, Integer size) {
        Properties props = buildRandomConsumerProps();

        try (KafkaConsumer<TradeKey, TradeValue> consumer = new KafkaConsumer<>(props)) {
            String topic = topicsConfiguration.getTrades();
            var partitionsInfo = consumer.partitionsFor(topic);

            List<TopicPartition> partitions = partitionsInfo.stream()
                .map(p -> new TopicPartition(topic, p.partition()))
                .toList();

            consumer.assign(partitions);

            Map<TopicPartition, Long> beginningOffsets = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            int totalRecords = (int) endOffsets.entrySet().stream()
                .mapToLong(e -> e.getValue() - beginningOffsets.get(e.getKey()))
                .sum();

            int totalPages = (int) Math.ceil((double) totalRecords / size);
            int fromIndex = (page - 1) * size;

            if (fromIndex >= totalRecords) {
                return new TradesPageVO(page, size, totalPages, totalRecords, List.of());
            }

            List<TradeVO> trades = new ArrayList<>();
            long currentIndex = 0;

            for (TopicPartition tp : partitions) {
                long start = beginningOffsets.get(tp) + fromIndex;
                long end = endOffsets.get(tp);

                if (start >= end) {
                    continue;
                }

                consumer.seek(tp, start);

                while (consumer.position(tp) < end) {
                    var consumerRecords = consumer.poll(Duration.ofMillis(200));
                    for (var tradeRecord : consumerRecords.records(tp)) {
                        if (currentIndex < size) {
                            TradeVO trade = tradeMapper.from(tradeRecord.key(), tradeRecord.value());
                            trades.add(trade);
                        }
                        currentIndex++;
                    }
                }
            }

            return new TradesPageVO(page, size, totalPages, totalRecords, trades);
        }
    }

    private Properties buildRandomConsumerProps() {
        Properties props = new Properties();
        props.putAll(kafkaProperties.buildConsumerProperties());
        props.put("group.id", "trades-consumer-" + UUID.randomUUID());
        return props;
    }
}
