package com.sixgroup.referencedata.domain.isin;

public interface IsinRepository {

    IsinVO persist(IsinVO isinVO);

    IsinVO getIsinData(String isin);

    IsinsPageVO getIsins(Integer page, Integer size);
}
