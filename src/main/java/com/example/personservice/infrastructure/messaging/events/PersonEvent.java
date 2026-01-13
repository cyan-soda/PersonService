package com.example.personservice.infrastructure.messaging.events;

import com.example.personservice.domain.model.Person;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PersonEvent extends DomainEvent<PersonEvent.EventType> {
    public enum EventType { CREATE, UPDATE, DELETE }

    private Person person;

    public PersonEvent() {
        super(null);
    }

    public PersonEvent(EventType eventType, Person person) {
        super(eventType);
        this.person = person;
    }
}