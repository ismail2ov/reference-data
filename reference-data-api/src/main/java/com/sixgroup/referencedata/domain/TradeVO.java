package com.sixgroup.referencedata.domain;

public record TradeVO(String tradeRef,
                      int securityId,
                      TradeType tradeType,
                      String isin,
                      long quantity,
                      long price,
                      long timestamp) {

}
