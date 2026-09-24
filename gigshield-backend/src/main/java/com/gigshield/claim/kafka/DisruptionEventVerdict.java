package com.gigshield.claim.kafka;


public record DisruptionEventVerdict(
        boolean genuine,
        boolean triggerConfirmed,
        boolean riskConfirmed,
        String reason) {
}
