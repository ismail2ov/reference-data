package com.sixgroup.referencedata.domain.trade;

import java.util.List;

public record TradesPageVO(Integer page, Integer size, Integer totalPages, Integer totalRecords, List<TradeVO> data) {

    public TradesPageVO withTrades(List<TradeVO> trades) {
        return new TradesPageVO(page, size, totalPages, totalRecords, trades);
    }
}
