package com.sixgroup.referencedata.infrastructure.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import com.sixgroup.referencedata.application.TradeService;
import com.sixgroup.referencedata.domain.TradeVO;
import com.sixgroup.referencedata.domain.TradesPageVO;
import com.sixgroup.referencedata.infrastructure.controller.api.TradesApi;
import com.sixgroup.referencedata.infrastructure.controller.model.TradeRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.TradesListRDTO;
import com.sixgroup.referencedata.infrastructure.mapper.TradeMapper;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class TradeController implements TradesApi {

    private final TradeService tradeService;
    private final TradeMapper tradeMapper;

    @Override
    public ResponseEntity<TradeRDTO> createTrade(TradeRDTO tradeRDTO) {
        TradeVO tradeVO = tradeMapper.from(tradeRDTO);
        TradeVO persisted = tradeService.createTrade(tradeVO);

        TradeRDTO response = tradeMapper.fromVO(persisted);

        return new ResponseEntity<>(response, HttpStatus.CREATED);
    }

    @Override
    public ResponseEntity<TradesListRDTO> getTrades(Integer page, Integer size) {
        TradesPageVO tradesPageVO = tradeService.getTrades(page, size);
        TradesListRDTO isinListRDTO = tradeMapper.fromPage(tradesPageVO);

        return ResponseEntity.ok(isinListRDTO);
    }
}
