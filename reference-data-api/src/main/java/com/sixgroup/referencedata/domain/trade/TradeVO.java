package com.sixgroup.referencedata.domain.trade;

public record TradeVO(String tradeRef,
                      int securityId,
                      TradeType tradeType,
                      String isin,
                      long quantity,
                      long price,
                      long timestamp) {

}
