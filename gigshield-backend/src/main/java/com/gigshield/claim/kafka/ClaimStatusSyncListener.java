package com.gigshield.claim.kafka;

import com.gigshield.claim.enums.ClaimStatus;
import com.gigshield.claim.repository.ClaimRepository;
import com.gigshield.kafka.KafkaTopics;
import com.gigshield.payment.kafka.PayoutCompletedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;


@Slf4j
@Component
@RequiredArgsConstructor
public class ClaimStatusSyncListener {

    private final ClaimRepository claimRepository;

    @KafkaListener(
            topics = KafkaTopics.PAYOUT_COMPLETED,
            groupId = KafkaTopics.GROUP_CLAIM_STATUS_SYNC,
            containerFactory = "kafkaListenerContainerFactory")
    public void onPayoutCompleted(PayoutCompletedEvent event) {
        claimRepository.findById(event.getClaimId()).ifPresentOrElse(claim -> {
            ClaimStatus next = "SUCCESS".equals(event.getStatus()) ? ClaimStatus.PAID : ClaimStatus.FAILED;

            // Idempotent — a redelivered event is a no-op once the claim is already terminal.
            if (claim.getStatus() == ClaimStatus.PAID || claim.getStatus() == ClaimStatus.FAILED) {
                return;
            }

            claim.setStatus(next);
            if (claim.getProcessedAt() == null) {
                claim.setProcessedAt(LocalDateTime.now());
            }
            claimRepository.save(claim);
            log.info("Claim {} synced to status={} from payout completion", claim.getId(), next);
        }, () -> log.warn("Payout-completed event referenced unknown claimId={}", event.getClaimId()));
    }
}
