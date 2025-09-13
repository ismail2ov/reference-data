package com.sixgroup.referencedata.infrastructure.messaging.repository;

import org.springframework.stereotype.Repository;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.domain.IsinRepository;
import com.sixgroup.referencedata.domain.IsinVO;
import com.sixgroup.referencedata.domain.exception.IsinNotFoundException;
import com.sixgroup.referencedata.infrastructure.mapper.IsinMapper;

import lombok.RequiredArgsConstructor;

@Repository
@RequiredArgsConstructor
public class IsinKafkaRepository implements IsinRepository {

    private final IsinKTableRepository isinKTableRepository;
    private final IsinPublisher isinPublisher;
    private final IsinMapper isinMapper;

    @Override
    public IsinVO persist(IsinVO isinVO) {
        IsinDataKey key = isinMapper.keyFrom(isinVO);
        IsinDataValue value = isinMapper.valueFrom(isinVO);
        isinPublisher.publishTrade(key, value);
        return isinVO;
    }

    @Override
    public IsinVO getIsinData(String isin) {
        return isinKTableRepository.findByKey(isin).orElseThrow(() -> new IsinNotFoundException("ISIN '{}' not found, isin"));
    }

}
