package com.gigshield.event.entity;

import com.gigshield.event.enums.EventStatus;
import com.gigshield.event.enums.EventType;
import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;

@Entity
@Table(name = "disruption_events",
        indexes = {
                @Index(name = "idx_event_city_type", columnList = "city, eventType"),
                @Index(name = "idx_event_active",    columnList = "status, startTime")
        })
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class DisruptionEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventType eventType;

    @Column(nullable = false)
    private String city;

    private Double metricValue;

    @Column(nullable = false)
    private LocalDateTime startTime;

    private LocalDateTime endTime;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status;

    private String sourceSystem;

    private String externalReference;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    void prePersist() {
        createdAt = LocalDateTime.now();
        status    = EventStatus.ACTIVE;
    }
}