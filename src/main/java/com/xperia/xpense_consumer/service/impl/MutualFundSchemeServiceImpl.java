package com.xperia.xpense_consumer.service.impl;

import com.xperia.xpense_consumer.service.MutualFundSchemeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xperia.entities.mf.MutualFundScheme;
import org.xperia.repository.mf.MutualFundSchemeRepository;

import java.util.List;

@Service
public class MutualFundSchemeServiceImpl implements MutualFundSchemeService {

    @Autowired
    private MutualFundSchemeRepository mfSchemeRepository;

    @Override
    public void saveAllSchemes(List<MutualFundScheme> schemes) {
        mfSchemeRepository.saveAll(schemes);
    }
}
