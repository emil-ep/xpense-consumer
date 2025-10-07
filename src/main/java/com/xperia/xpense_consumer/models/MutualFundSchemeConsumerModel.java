package com.xperia.xpense_consumer.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class MutualFundSchemeConsumerModel implements Serializable {

    private String schemeCode;

    private String schemeName;

    private String isinGrowth;

    private String isinDivReinvestment;

    @Override
    public String toString() {
        return "MutualFundScheme{" +
                "schemeCode='" + schemeCode + '\'' +
                ", schemeName='" + schemeName + '\'' +
                ", isinGrowth='" + isinGrowth + '\'' +
                ", isinDivReinvestment='" + isinDivReinvestment + '\'' +
                '}';
    }
}
