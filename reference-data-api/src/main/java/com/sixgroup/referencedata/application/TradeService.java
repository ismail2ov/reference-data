package com.sixgroup.referencedata.application;

import com.sixgroup.referencedata.domain.TradeRepository;
import com.sixgroup.referencedata.domain.TradeVO;
import com.sixgroup.referencedata.domain.TradesPageVO;

public class TradeService {

    private final TradeRepository tradeRepository;

    public TradeService(TradeRepository tradeRepository) {
        this.tradeRepository = tradeRepository;
    }

    public TradeVO createTrade(TradeVO tradeVO) {
        return tradeRepository.persist(tradeVO);
    }

    public TradesPageVO getTrades(Integer page, Integer size) {
        return tradeRepository.getTrades(page, size);
    }
}
