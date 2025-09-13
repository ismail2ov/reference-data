package com.sixgroup.referencedata.infrastructure.mapper;

import org.mapstruct.Mapper;

import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.domain.TradeVO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeRDTO;

@Mapper
public interface TradeMapper {

    TradeVO from(TradeRDTO tradeRDTO);

    TradeRDTO fromVO(TradeVO persisted);

    TradeKey keyFrom(TradeVO tradeVO);

    TradeValue valueFrom(TradeVO tradeVO);
}
