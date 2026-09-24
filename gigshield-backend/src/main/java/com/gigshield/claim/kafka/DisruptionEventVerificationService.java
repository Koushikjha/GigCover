package com.gigshield.claim.kafka;

import com.gigshield.config.AppConstants;
import com.gigshield.event.enums.EventType;
import com.gigshield.integration.MlServiceClient;
import com.gigshield.integration.dto.TriggerCheckResponse;
import com.gigshield.risk.dto.RiskScoreRequest;
import com.gigshield.risk.dto.RiskScoreResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Locale;


@Slf4j
@Component
@RequiredArgsConstructor
public class DisruptionEventVerificationService {

    private final MlServiceClient mlServiceClient;

    public DisruptionEventVerdict verify(EventType eventType, String city,
                                          Double latitude, Double longitude,
                                          LocalDateTime occurredAt) {
        if (eventType == EventType.WAR) {
            return new DisruptionEventVerdict(true, true, true,
                    "WAR event — ML verification bypassed per product spec");
        }

        TriggerCheckResponse triggerCheck =
                mlServiceClient.getTriggerCheck(city, latitude, longitude, occurredAt);
        boolean triggerConfirmed = triggerCheck.getActiveTriggers() != null
                && triggerCheck.getActiveTriggers().stream()
                        .anyMatch(t -> t.equalsIgnoreCase(eventType.name()));

        RiskScoreResponse riskScore = null;
        if (latitude != null && longitude != null) {
            riskScore = mlServiceClient.getRiskScore(RiskScoreRequest.builder()
                    .city(city)
                    .latitude(latitude)
                    .longitude(longitude)
                    .platform("AGGREGATE")
                    .at(occurredAt)
                    .build());
        } else {
            log.warn("No coordinates available for city={} event={} — skipping independent risk-score corroboration",
                    city, eventType);
        }

        boolean riskConfirmed = riskScore != null
                && riskScore.getRiskScore() != null
                && riskScore.getRiskScore() >= AppConstants.EVENT_GENUINE_MIN_RISK_SCORE;

        boolean genuine = triggerConfirmed && riskConfirmed;

        String reason = String.format(Locale.ROOT,
                "occurredAt=%s triggerConfirmed=%s riskConfirmed=%s (riskScore=%s, band=%s)",
                occurredAt != null ? occurredAt : "now",
                triggerConfirmed, riskConfirmed,
                riskScore != null ? riskScore.getRiskScore() : "n/a",
                riskScore != null ? riskScore.getRiskBand() : "n/a");

        return new DisruptionEventVerdict(genuine, triggerConfirmed, riskConfirmed, reason);
    }
}
