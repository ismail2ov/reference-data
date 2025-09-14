package com.sixgroup.referencedata.application;

import com.sixgroup.referencedata.domain.trade.TradeRepository;
import com.sixgroup.referencedata.domain.trade.TradeVO;
import com.sixgroup.referencedata.domain.trade.TradesPageVO;

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
