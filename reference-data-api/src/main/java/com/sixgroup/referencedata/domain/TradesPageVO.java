package com.sixgroup.referencedata.domain;

import java.util.List;

public record TradesPageVO(Integer page, Integer size, Integer totalPages, Integer totalRecords, List<TradeVO> data) {

}
