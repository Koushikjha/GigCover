package com.gigshield.integration.dto;

import lombok.Data;
import java.util.List;

@Data
public class TriggerCheckResponse {

    private String city;

    private List<String> activeTriggers;

    private java.util.Map<String, Double> metricValues;
}