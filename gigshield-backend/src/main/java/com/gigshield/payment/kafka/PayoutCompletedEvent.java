package com.gigshield.payment.kafka;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PayoutCompletedEvent {
    private String         claimId;
    private Long           userId;
    private Integer        amountInr;
    private String         status;
    private LocalDateTime  completedAt;
}
