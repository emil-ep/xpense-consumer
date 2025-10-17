package com.xperia.xpense_consumer.models.entity;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;

import java.sql.Types;

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

    @JdbcTypeCode(Types.OTHER)
    @Column(columnDefinition = "jsonb")
    private JsonNode payload;

    public MutualFundSchemeDetail(String code, String type, String category, String fundHouse, String name, JsonNode payload){
        this.code = code;
        this.type = type;
        this.category = category;
        this.fundHouse = fundHouse;
        this.name = name;
        this.payload = payload;
    }
}
