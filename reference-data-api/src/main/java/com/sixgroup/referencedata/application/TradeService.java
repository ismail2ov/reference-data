package com.sixgroup.referencedata.application;

import com.sixgroup.referencedata.domain.TradeRepository;
import com.sixgroup.referencedata.domain.TradeVO;

public class TradeService {

    private final TradeRepository tradeRepository;

    public TradeService(TradeRepository tradeRepository) {
        this.tradeRepository = tradeRepository;
    }

    public TradeVO createTrade(TradeVO tradeVO) {
        return tradeRepository.persist(tradeVO);
    }

}
