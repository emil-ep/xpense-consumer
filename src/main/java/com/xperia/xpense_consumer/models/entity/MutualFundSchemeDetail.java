package com.xperia.xpense_consumer.models.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Entity(name = "mf_scheme_detail")
@NoArgsConstructor
@Getter
public class MutualFundSchemeDetail {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;

    private String code;

    private String type;

    private String category;

    private String fundHouse;

    private String name;

    public MutualFundSchemeDetail(String code, String type, String category, String fundHouse, String name){
        this.code = code;
        this.type = type;
        this.category = category;
        this.fundHouse = fundHouse;
        this.name = name;
    }
}
