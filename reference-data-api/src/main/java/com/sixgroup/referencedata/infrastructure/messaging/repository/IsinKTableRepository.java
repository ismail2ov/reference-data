package com.sixgroup.referencedata.infrastructure.messaging.repository;

import java.util.Objects;
import java.util.Optional;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Repository;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.domain.IsinVO;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.KafkaStreamsConfig;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Repository
@RequiredArgsConstructor
@Slf4j
public class IsinKTableRepository {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public Optional<IsinVO> findByKey(String isin) {
        ReadOnlyKeyValueStore<IsinDataKey, IsinDataValue> keyValueStore = Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
            .store(StoreQueryParameters.fromNameAndType(
                KafkaStreamsConfig.ISIN_STORE,
                QueryableStoreTypes.keyValueStore()
            ));

        IsinDataValue value = keyValueStore.get(IsinDataKey.newBuilder().setIsin(isin).build());

        return Optional.ofNullable(value)
            .map(v -> new IsinVO(isin, v.getMaturityDate(), v.getCurrency().toString(), v.getCfi().toString()));
    }

}
