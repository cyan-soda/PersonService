package com.example.personservice.domain.specification;

import com.example.personservice.domain.model.Person;
import jakarta.persistence.criteria.Predicate;
import org.springframework.data.jpa.domain.Specification;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class PersonSpecification {
    public static Specification<Person> hasNameAndAge(
            String firstName,
            String lastName,
            Integer minAge
    ) {
        return (root, query, cb) -> {
            List<Predicate> predicates = new ArrayList<>();

            if (firstName != null && !firstName.isBlank()) {
                predicates.add(cb.like(cb.lower(root.get("firstName")), firstName));
            }

            if (lastName != null && !lastName.isBlank()) {
                predicates.add(cb.like(cb.lower(root.get("firstName")), lastName));
            }

            if (minAge != null) {
                LocalDate date = LocalDate.now().minusYears(minAge);
                predicates.add(cb.lessThanOrEqualTo(root.get("dateOfBirth"), date));
            }

            return cb.and(predicates.toArray(new Predicate[0]));
        };
    }
}
