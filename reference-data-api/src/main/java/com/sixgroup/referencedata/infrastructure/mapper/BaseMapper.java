package com.sixgroup.referencedata.infrastructure.mapper;

import org.mapstruct.Mapper;

@Mapper
public interface BaseMapper {

    default String map(CharSequence cs) {
        return cs == null ? null : cs.toString();
    }

}
