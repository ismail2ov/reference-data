package com.sixgroup.referencedata.domain;

public interface IsinRepository {

    IsinVO persist(IsinVO isinVO);

    IsinVO getIsinData(String isin);

}
