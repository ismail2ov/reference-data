package com.sixgroup.referencedata.application;

import com.sixgroup.referencedata.domain.IsinRepository;
import com.sixgroup.referencedata.domain.IsinVO;

public class IsinService {

    private final IsinRepository isinRepository;

    public IsinService(IsinRepository isinRepository) {
        this.isinRepository = isinRepository;
    }

    public IsinVO createIsin(IsinVO isinVO) {
        return isinRepository.persist(isinVO);
    }
}
