package com.sixgroup.referencedata.domain.trade;

public interface TradeRepository {

    TradeVO persist(TradeVO tradeVO);

    TradesPageVO getTrades(Integer page, Integer size);
}
