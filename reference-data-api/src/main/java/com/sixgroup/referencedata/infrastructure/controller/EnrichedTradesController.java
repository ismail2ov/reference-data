package com.sixgroup.referencedata.infrastructure.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import com.sixgroup.referencedata.application.EnrichedTradeService;
import com.sixgroup.referencedata.domain.enriched.EnrichedTradeVO;
import com.sixgroup.referencedata.infrastructure.controller.api.EnrichedTradesApi;
import com.sixgroup.referencedata.infrastructure.controller.model.EnrichedTradeRDTO;
import com.sixgroup.referencedata.infrastructure.mapper.EnrichedTradeMapper;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class EnrichedTradesController implements EnrichedTradesApi {

    private final EnrichedTradeService enrichedTradeService;
    private final EnrichedTradeMapper enrichedTradeMapper;

    @Override
    public ResponseEntity<EnrichedTradeRDTO> getEnrichedTrade(String tradeRef) {
        EnrichedTradeVO enrichedTradeVO = enrichedTradeService.getEnrichedTrade(tradeRef);
        EnrichedTradeRDTO enrichedTradeRDTO = enrichedTradeMapper.from(enrichedTradeVO);

        return ResponseEntity.ok(enrichedTradeRDTO);
    }
}
