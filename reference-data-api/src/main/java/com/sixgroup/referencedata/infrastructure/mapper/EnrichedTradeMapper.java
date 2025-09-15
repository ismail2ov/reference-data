package com.sixgroup.referencedata.infrastructure.mapper;

import org.mapstruct.Mapper;

import com.sixgroup.avro.enriched.trade.EnrichedTradeValue;
import com.sixgroup.referencedata.domain.enriched.EnrichedTradeVO;
import com.sixgroup.referencedata.infrastructure.controller.model.EnrichedTradeRDTO;

@Mapper(uses = BaseMapper.class)
public interface EnrichedTradeMapper {

    EnrichedTradeRDTO from(EnrichedTradeVO enrichedTradeVO);

    EnrichedTradeVO from(String tradeRef, EnrichedTradeValue v);
}
