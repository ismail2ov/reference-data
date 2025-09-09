package com.sixgroup.referencedata.infrastructure.controller;

import java.net.URI;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;

import com.sixgroup.referencedata.application.IsinService;
import com.sixgroup.referencedata.domain.IsinVO;
import com.sixgroup.referencedata.infrastructure.configuration.TopicsConfiguration;
import com.sixgroup.referencedata.infrastructure.controller.api.IsinsApi;
import com.sixgroup.referencedata.infrastructure.controller.model.GetIsins200ResponseRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinRDTO;
import com.sixgroup.referencedata.infrastructure.mapper.IsinMapper;

import lombok.RequiredArgsConstructor;

@RestController
@RequiredArgsConstructor
public class IsinController implements IsinsApi {

    private final IsinService isinService;
    private final IsinMapper isinMapper;
    private final TopicsConfiguration topicsConfiguration;

    @Override
    public ResponseEntity<Void> createIsin(IsinRDTO isinRDTO) {
        IsinVO isinVO = isinMapper.from(isinRDTO);
        IsinVO persisted = isinService.createIsin(isinVO);

        URI location = URI.create("/isins/" + persisted.isin());
        return ResponseEntity.created(location).build();
    }

    @Override
    public ResponseEntity<List<IsinRDTO>> getIsin(String isin) {
        return null;
    }

    @Override
    public ResponseEntity<GetIsins200ResponseRDTO> getIsins(Integer page, Integer size) {
        return null;
    }
}
