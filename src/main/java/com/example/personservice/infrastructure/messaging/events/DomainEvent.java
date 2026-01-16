package com.example.personservice.infrastructure.messaging.events;

import lombok.Getter;
import lombok.Setter;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@Getter
@Setter
public abstract class DomainEvent<T extends Enum<T>> {
    private final UUID eventId;
    private final T eventType;
    private final LocalDateTime occurredOn;

    protected DomainEvent(T eventType) {
        this.eventId = UUID.randomUUID();
        this.eventType = eventType;
        this.occurredOn = LocalDateTime.now();
    }
}