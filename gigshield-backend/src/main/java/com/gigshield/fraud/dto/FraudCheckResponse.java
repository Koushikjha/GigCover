package com.gigshield.fraud.dto;

import lombok.*;

@Data
public class FraudCheckResponse {
    private Integer fraudScore;
    private String  recommendation;
}