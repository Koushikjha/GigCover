package com.gigshield.claim.dto;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.time.LocalDateTime;


@Data
public class ReportCancelledOrderRequest {


    @NotNull(message = "cancelledAt is required — when did the order get cancelled?")
    private LocalDateTime cancelledAt;

    @Size(max = 280, message = "note must be 280 characters or fewer")
    private String note;
}
