package com.sixgroup.referencedata.integration.utils;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.awaitility.Awaitility;

public class KafkaConsumerTestUtils<K, V> implements AutoCloseable {

    private final Consumer<K, V> consumer;
    private final List<ConsumerRecord<K, V>> records = new CopyOnWriteArrayList<>();

    public KafkaConsumerTestUtils(Consumer<K, V> consumer, List<String> topics) {
        this.consumer = consumer;
        this.consumer.subscribe(topics);
        startPolling();
    }

    private void startPolling() {
        Thread.startVirtualThread(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    consumer.poll(Duration.ofMillis(500))
                        .forEach(records::add);
                }
            } catch (WakeupException e) {
                // WakeupException
            } finally {
                consumer.close();
            }
        });
    }

    public List<ConsumerRecord<K, V>> getAllRecords() {
        return List.copyOf(records);
    }

    public List<ConsumerRecord<K, V>> findAllRecordsByKey(String searchText) {
        return records.stream()
            .filter(r -> r.key().toString().contains(searchText))
            .toList();
    }

    public List<ConsumerRecord<K, V>> findAllRecordsByKey(String searchText, Duration timeout) {
        Awaitility.await()
            .atMost(timeout.toMillis(), TimeUnit.MILLISECONDS)
            .pollInterval(200, TimeUnit.MILLISECONDS)
            .until(() -> !findAllRecordsByKey(searchText).isEmpty());

        return findAllRecordsByKey(searchText);
    }

    @Override
    public void close() {
        consumer.wakeup();
    }
}
