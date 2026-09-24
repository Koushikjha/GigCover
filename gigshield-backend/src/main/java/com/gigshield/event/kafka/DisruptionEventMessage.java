package com.gigshield.event.kafka;

import com.gigshield.event.enums.EventType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DisruptionEventMessage {
    private Long          eventId;
    private EventType     eventType;
    private String        city;
    private Double        metricValue;
    private String        sourceSystem;
    private String        externalReference;
    private LocalDateTime occurredAt;
}
