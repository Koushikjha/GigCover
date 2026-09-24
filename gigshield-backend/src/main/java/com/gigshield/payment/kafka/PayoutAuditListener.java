package com.gigshield.payment.kafka;

import com.gigshield.kafka.KafkaTopics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class PayoutAuditListener {

    @KafkaListener(
            topics = KafkaTopics.PAYOUT_COMPLETED,
            groupId = KafkaTopics.GROUP_AUDIT,
            containerFactory = "kafkaListenerContainerFactory")
    public void onPayoutCompleted(PayoutCompletedEvent event) {
        log.info("[AUDIT] payout claimId={} userId={} amount=₹{} status={} completedAt={}",
                event.getClaimId(), event.getUserId(), event.getAmountInr(),
                event.getStatus(), event.getCompletedAt());
    }
}
