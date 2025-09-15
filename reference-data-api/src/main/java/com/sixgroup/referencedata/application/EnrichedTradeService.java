package com.sixgroup.referencedata.application;

import com.sixgroup.referencedata.domain.enriched.EnrichedTradeRepository;
import com.sixgroup.referencedata.domain.enriched.EnrichedTradeVO;

public class EnrichedTradeService {

    private final EnrichedTradeRepository enrichedTradeRepository;

    public EnrichedTradeService(EnrichedTradeRepository enrichedTradeRepository) {
        this.enrichedTradeRepository = enrichedTradeRepository;
    }

    public EnrichedTradeVO getEnrichedTrade(String tradeRef) {
        return enrichedTradeRepository.getEnrichedTradeBy(tradeRef);
    }
}
