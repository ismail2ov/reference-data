package com.sixgroup.referencedata.domain.isin;

import java.util.List;

public record IsinsPageVO(Integer page, Integer size, Integer totalPages, Integer totalRecords, List<IsinVO> data) {

}
