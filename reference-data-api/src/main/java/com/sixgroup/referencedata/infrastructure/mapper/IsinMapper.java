package com.sixgroup.referencedata.infrastructure.mapper;

import org.mapstruct.Mapper;

import com.sixgroup.avro.isin.data.IsinDataKey;
import com.sixgroup.avro.isin.data.IsinDataValue;
import com.sixgroup.referencedata.domain.IsinVO;
import com.sixgroup.referencedata.domain.IsinsPageVO;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinListRDTO;
import com.sixgroup.referencedata.infrastructure.controller.model.IsinRDTO;

@Mapper
public interface IsinMapper {

    IsinVO from(IsinRDTO isinRDTO);

    IsinDataKey keyFrom(IsinVO isinVO);

    IsinDataValue valueFrom(IsinVO isinVO);

    IsinRDTO from(IsinVO isinVO);

    IsinListRDTO fromPage(IsinsPageVO isinsPageVO);

    IsinVO from(String isin, IsinDataValue value);

    IsinVO from(IsinDataKey key, IsinDataValue value);

    default String map(CharSequence cs) {
        return cs == null ? null : cs.toString();
    }
}
