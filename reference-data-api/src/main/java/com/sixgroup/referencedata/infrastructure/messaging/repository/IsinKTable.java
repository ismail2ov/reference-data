package com.sixgroup.referencedata.infrastructure.messaging.repository;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Repository;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.domain.IsinVO;
import com.sixgroup.referencedata.domain.IsinsPageVO;
import com.sixgroup.referencedata.infrastructure.mapper.IsinMapper;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.KafkaStreamsConfig;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Repository
@RequiredArgsConstructor
@Slf4j
public class IsinKTable {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final IsinMapper isinMapper;

    public Optional<IsinVO> findByKey(String isin) {
        ReadOnlyKeyValueStore<IsinDataKey, IsinDataValue> keyValueStore = getFromStore();

        IsinDataValue value = keyValueStore.get(IsinDataKey.newBuilder().setIsin(isin).build());

        return Optional.ofNullable(value)
            .map(v -> isinMapper.from(isin, v));
    }

    public IsinsPageVO getIsins(Integer page, Integer size) {
        List<IsinVO> allIsins = getIsinVOList();

        int totalRecords = allIsins.size();
        int totalPages = (int) Math.ceil((double) totalRecords / size);

        int fromIndex = (page - 1) * size;
        if (fromIndex >= totalRecords) {
            return new IsinsPageVO(page, size, totalPages, totalRecords, List.of());
        }

        int toIndex = Math.min(fromIndex + size, totalRecords);
        List<IsinVO> data = allIsins.subList(fromIndex, toIndex);

        return new IsinsPageVO(page, size, totalPages, totalRecords, data);
    }

    private List<IsinVO> getIsinVOList() {
        ReadOnlyKeyValueStore<IsinDataKey, IsinDataValue> keyValueStore = getFromStore();

        try (KeyValueIterator<IsinDataKey, IsinDataValue> it = keyValueStore.all()) {
            return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false)
                .map(e -> isinMapper.from(e.key, e.value))
                .toList();
        }
    }

    private ReadOnlyKeyValueStore<IsinDataKey, IsinDataValue> getFromStore() {
        return Objects.requireNonNull(
            streamsBuilderFactoryBean.getKafkaStreams())
            .store(StoreQueryParameters.fromNameAndType(
                KafkaStreamsConfig.ISIN_STORE,
                QueryableStoreTypes.keyValueStore()
            ));
    }
}
