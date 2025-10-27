package com.xperia.xpense_consumer.service.impl;

import com.xperia.xpense_consumer.service.MutualFundSchemeDetailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.xperia.entities.mf.MutualFundSchemeDetail;
import org.xperia.repository.mf.MutualFundSchemeDetailRepository;

import java.util.List;

@Service
public class MutualFundSchemeDetailServiceImpl implements MutualFundSchemeDetailService {

    @Autowired
    private MutualFundSchemeDetailRepository repository;

    @Override
    public List<MutualFundSchemeDetail> saveAll(List<MutualFundSchemeDetail> schemes) {
        if (!schemes.isEmpty()){
            return repository.saveAll(schemes);
        }
        return List.of();
    }

    @Override
    public MutualFundSchemeDetail save(MutualFundSchemeDetail scheme) {
        return null;
    }
}
