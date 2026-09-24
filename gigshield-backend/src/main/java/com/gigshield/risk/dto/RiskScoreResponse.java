package com.gigshield.risk.dto;

import lombok.*;

@Data
public class RiskScoreResponse {
    private Double riskScore;
    private Integer recommendedPremium;
    private String  riskBand;
}