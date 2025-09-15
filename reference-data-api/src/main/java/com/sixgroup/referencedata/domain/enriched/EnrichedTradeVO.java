package com.sixgroup.referencedata.domain.enriched;

import java.time.LocalDate;

import com.sixgroup.referencedata.domain.trade.TradeType;

public record EnrichedTradeVO(String tradeRef,
                              int securityId,
                              TradeType tradeType,
                              long quantity,
                              long price,
                              String currency,
                              LocalDate maturityDate,
                              String cfi,
                              long timestamp) {

}
