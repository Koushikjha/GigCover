package com.gigshield.claim.kafka;

import com.gigshield.claim.service.ClaimService;
import com.gigshield.event.enums.EventType;
import com.gigshield.event.kafka.DisruptionEventMessage;
import com.gigshield.kafka.KafkaTopics;
import com.gigshield.policy.entity.Policy;
import com.gigshield.policy.repository.PolicyRepository;
import com.gigshield.user.entity.User;
import com.gigshield.user.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;


@Slf4j
@Component
@RequiredArgsConstructor
public class DisruptionEventListener {

    private final PolicyRepository policyRepository;
    private final ClaimService     claimService;
    private final UserService      userService;
    private final DisruptionEventVerificationService verificationService;

    @KafkaListener(
            topics = KafkaTopics.DISRUPTION_EVENT_CREATED,
            groupId = KafkaTopics.GROUP_CLAIMS_AUTOMATION,
            containerFactory = "kafkaListenerContainerFactory")
    public void onDisruptionEvent(DisruptionEventMessage message) {
        log.info("Claims automation received disruption event id={} type={} city={}",
                message.getEventId(), message.getEventType(), message.getCity());

        if (message.getEventType() == EventType.ORDER_CANCELLED) {
            log.warn("Disruption event id={} has type ORDER_CANCELLED — this should never reach the automated "
                            + "pipeline (it's worker-reported only, see ClaimService#reportCancelledOrder). Skipping.",
                    message.getEventId());
            return;
        }

        List<Policy> activePolicies =
                policyRepository.findActivePoliciesByCity(message.getCity(), LocalDate.now());

        if (activePolicies.isEmpty()) {
            log.info("No active policies in city={} — nothing to auto-claim for event id={}",
                    message.getCity(), message.getEventId());
            return;
        }

        Set<Long> userIds = activePolicies.stream()
                .map(Policy::getUserId)
                .collect(Collectors.toSet());

        double[] coordinates = averageCoordinates(userIds);

        DisruptionEventVerdict verdict = verificationService.verify(
                message.getEventType(), message.getCity(),
                coordinates == null ? null : coordinates[0],
                coordinates == null ? null : coordinates[1],
                message.getOccurredAt());
        if (!verdict.genuine()) {
            log.warn("Disruption event id={} type={} city={} failed ML verification ({}) — "
                            + "holding automation, no claims will be auto-created for this event",
                    message.getEventId(), message.getEventType(), message.getCity(), verdict.reason());
            return;
        }
        log.info("Disruption event id={} verified genuine ({}) — proceeding to claims automation",
                message.getEventId(), verdict.reason());

        int created = 0, skipped = 0;
        for (Long userId : userIds) {
            try {
                claimService.processParametricClaim(userId, message.getEventType());
                created++;
            } catch (IllegalStateException e) {
                log.debug("Skipping auto-claim for userId={} event={}: {}",
                        userId, message.getEventType(), e.getMessage());
                skipped++;
            } catch (Exception e) {
                log.error("Auto-claim failed for userId={} event={} city={}: {}",
                        userId, message.getEventType(), message.getCity(), e.getMessage());
                skipped++;
            }
        }

        log.info("Claims automation done for event id={} city={}: {} created, {} skipped",
                message.getEventId(), message.getCity(), created, skipped);
    }

    private double[] averageCoordinates(Set<Long> userIds) {
        double latSum = 0, lonSum = 0;
        int counted = 0;
        for (Long userId : userIds) {
            try {
                User user = userService.findById(userId);
                if (user.getLatitude() != null && user.getLongitude() != null) {
                    latSum += user.getLatitude();
                    lonSum += user.getLongitude();
                    counted++;
                }
            } catch (Exception e) {
                log.debug("Could not resolve coordinates for userId={}: {}", userId, e.getMessage());
            }
        }
        if (counted == 0) {
            return null;
        }
        return new double[]{latSum / counted, lonSum / counted};
    }
}
