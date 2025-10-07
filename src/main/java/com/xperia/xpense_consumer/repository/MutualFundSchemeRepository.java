package com.xperia.xpense_consumer.repository;

import com.xperia.xpense_consumer.models.entity.MutualFundScheme;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface MutualFundSchemeRepository extends JpaRepository<MutualFundScheme, String> {

}
