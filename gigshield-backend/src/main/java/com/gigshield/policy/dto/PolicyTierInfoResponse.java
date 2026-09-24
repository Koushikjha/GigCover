package com.gigshield.policy.dto;

import com.gigshield.policy.enums.PolicyTier;
import lombok.*;

@Data @Builder
public class PolicyTierInfoResponse {
    private PolicyTier tier;
    private int        weeklyPremiumInr;
    private double     payoutRatio;
    private int        estimatedPayoutInr;
    private String     description;
}