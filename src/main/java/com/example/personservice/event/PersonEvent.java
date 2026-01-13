package com.example.personservice.event;

import com.example.personservice.domain.model.Person;
import lombok.*;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class PersonEvent extends DomainEvent {
    public enum EventType { CREATE, UPDATE, DELETE }

    private EventType action;
    private Person person;

    public PersonEvent() {
        super(null);
    }

    public PersonEvent(EventType action, Person person) {
        super(action.name());
        this.action = action;
        this.person = person;
    }
}