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

            int skipRecords = (page - 1) * size;
            TradesPageVO tradesPageStats = calculateTotals(page, size, beginningOffsets, endOffsets);

            if (skipRecords >= tradesPageStats.totalRecords()) {
                return tradesPageStats;
            }

            List<TradeVO> trades = new ArrayList<>();

            for (TopicPartition tp : partitions) {
                int tpNumRecords = (int) (endOffsets.get(tp) - beginningOffsets.get(tp));
                if (skipRecords > tpNumRecords) {
                    skipRecords -= tpNumRecords;
                } else {
                    long start = beginningOffsets.get(tp) + skipRecords;
                    long end = endOffsets.get(tp);
                    if (start < end) {
                        int maxRecords = size - trades.size();
                        List<TradeVO> recordsFromPartition = consumeRecordsFromPartition(consumer, tp, start, end, maxRecords);
                        trades.addAll(recordsFromPartition);
                    }
                }
            }

            return tradesPageStats.withTrades(trades);
        }
    }

    private List<TradeVO> consumeRecordsFromPartition(KafkaConsumer<TradeKey, TradeValue> consumer, TopicPartition topicPartition, long start,
        long end, int maxRecords) {
        List<TradeVO> trades = new ArrayList<>();

        consumer.seek(topicPartition, start);

        while (consumer.position(topicPartition) < end) {
            var consumerRecords = consumer.poll(Duration.ofMillis(200));
            for (var tradeRecord : consumerRecords.records(topicPartition)) {
                TradeVO trade = tradeMapper.from(tradeRecord.key(), tradeRecord.value());
                trades.add(trade);

                if (trades.size() >= maxRecords) {
                    return trades;
                }
            }
        }

        return trades;
    }

    private TradesPageVO calculateTotals(Integer page, Integer size, Map<TopicPartition, Long> beginningOffsets,
        Map<TopicPartition, Long> endOffsets) {

        int totalRecords = (int) endOffsets.entrySet().stream()
            .mapToLong(e -> e.getValue() - beginningOffsets.get(e.getKey()))
            .sum();

        int totalPages = (int) Math.ceil((double) totalRecords / size);

        return new TradesPageVO(page, size, totalPages, totalRecords, List.of());
    }

    private Properties buildRandomConsumerProps() {
        Properties props = new Properties();
        props.putAll(kafkaProperties.buildConsumerProperties());
        props.put("group.id", "trades-consumer-" + UUID.randomUUID());
        return props;
    }
}
