package com.gigshield.risk.dto;

import lombok.*;

import java.time.LocalDateTime;

@Data @Builder
public class RiskScoreRequest {
    private String city;
    private Double latitude;
    private Double longitude;
    private String platform;


    private LocalDateTime at;
}