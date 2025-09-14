package com.sixgroup.referencedata.infrastructure.mapper;

import org.mapstruct.Mapper;

import com.sixgroup.avro.trade.TradeKey;
import com.sixgroup.avro.trade.TradeValue;
import com.sixgroup.referencedata.domain.trade.TradeVO;
import com.sixgroup.referencedata.domain.trade.TradesPageVO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradesListRDTO;

@Mapper(uses = BaseMapper.class)
public interface TradeMapper {

    TradeVO from(TradeRDTO tradeRDTO);

    TradeRDTO fromVO(TradeVO persisted);

    TradeKey keyFrom(TradeVO tradeVO);

    TradeValue valueFrom(TradeVO tradeVO);

    TradesListRDTO fromPage(TradesPageVO tradesPageVO);

    TradeVO from(TradeKey key, TradeValue value);
}
