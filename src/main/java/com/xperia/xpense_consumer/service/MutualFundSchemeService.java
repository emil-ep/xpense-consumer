package com.xperia.xpense_consumer.service;

import com.xperia.xpense_consumer.models.entity.MutualFundScheme;

import java.util.List;

public interface MutualFundSchemeService {

    void saveAllSchemes(List<MutualFundScheme> schemes);
}
