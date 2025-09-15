package com.sixgroup.referencedata.infrastructure.messaging.repository.enriched;

import java.util.Objects;
import java.util.Optional;

import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Repository;

import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.sixgroup.avro.enriched.trade.EnrichedTradeKey;
import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.referencedata.domain.enriched.EnrichedTradeRepository;
import com.sixgroup.referencedata.domain.enriched.EnrichedTradeVO;
import com.sixgroup.referencedata.domain.exception.EnrichedTradeNotFoundException;
import com.sixgroup.referencedata.infrastructure.mapper.EnrichedTradeMapper;
import com.sixgroup.referencedata.infrastructure.messaging.kafka.KafkaStreamsConfig;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class EnrichedTradeKafkaRepository implements EnrichedTradeRepository {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    private final EnrichedTradeMapper enrichedTradeMapper;

    @Override
    public EnrichedTradeVO getEnrichedTradeBy(String tradeRef) {
        return this.findByKey(tradeRef).orElseThrow(() -> new EnrichedTradeNotFoundException("Enriched Trade with tradeRef '%s' not found".formatted(
            tradeRef)));
    }

    private Optional<EnrichedTradeVO> findByKey(String tradeRef) {
        ReadOnlyKeyValueStore<EnrichedTradeKey, EnrichedTradeValue> keyValueStore = getFromStore();

        EnrichedTradeValue value = keyValueStore.get(EnrichedTradeKey.newBuilder().setTradeRef(tradeRef).build());

        return Optional.ofNullable(value)
            .map(v -> enrichedTradeMapper.from(tradeRef, v));
    }

    private ReadOnlyKeyValueStore<EnrichedTradeKey, EnrichedTradeValue> getFromStore() {
        return Objects.requireNonNull(
            streamsBuilderFactoryBean.getKafkaStreams())
            .store(StoreQueryParameters.fromNameAndType(
                KafkaStreamsConfig.ENRICHED_TRADE_STORE,
                QueryableStoreTypes.keyValueStore()
            ));
    }
}
