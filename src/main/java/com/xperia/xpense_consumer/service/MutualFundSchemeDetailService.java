package com.xperia.xpense_consumer.service;

import com.xperia.xpense_consumer.models.entity.MutualFundSchemeDetail;

import java.util.List;

public interface MutualFundSchemeDetailService {

    List<MutualFundSchemeDetail> saveAll(List<MutualFundSchemeDetail> schemes);

    MutualFundSchemeDetail save(MutualFundSchemeDetail scheme);
}
