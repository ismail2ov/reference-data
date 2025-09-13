package com.sixgroup.referencedata.domain;

import java.util.List;

public record IsinsPageVO(Integer page, Integer size, Integer totalPages, Integer totalRecords, List<IsinVO> data) {

}
