package com.xperia.xpense_consumer.repository;

import com.xperia.xpense_consumer.models.entity.MutualFundSchemeDetail;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MutualFundSchemeDetailRepository extends JpaRepository<MutualFundSchemeDetail, String> {
}
