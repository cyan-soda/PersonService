package com.example.personservice.event;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Getter
@Setter
public abstract class DomainEvent {
    private final UUID eventId;
    private final String eventType;
    private final Instant occurredOn;

    protected DomainEvent(String eventType) {
        this.eventId = UUID.randomUUID();
        this.eventType = eventType;
        this.occurredOn = Instant.now();
    }
}