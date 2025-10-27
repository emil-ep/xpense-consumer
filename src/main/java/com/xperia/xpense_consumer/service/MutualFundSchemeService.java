package com.xperia.xpense_consumer.service;


import org.xperia.entities.mf.MutualFundScheme;

import java.util.List;

public interface MutualFundSchemeService {

    void saveAllSchemes(List<MutualFundScheme> schemes);
}
