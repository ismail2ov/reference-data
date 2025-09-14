package com.sixgroup.referencedata.application;

import com.sixgroup.referencedata.domain.isin.IsinRepository;
import com.sixgroup.referencedata.domain.isin.IsinVO;
import com.sixgroup.referencedata.domain.isin.IsinsPageVO;

public class IsinService {

    private final IsinRepository isinRepository;

    public IsinService(IsinRepository isinRepository) {
        this.isinRepository = isinRepository;
    }

    public IsinVO createIsin(IsinVO isinVO) {
        return isinRepository.persist(isinVO);
    }

    public IsinVO getIsinData(String isin) {
        return isinRepository.getIsinData(isin);
    }

    public IsinsPageVO getIsins(Integer page, Integer size) {
        return isinRepository.getIsins(page, size);
    }
}
