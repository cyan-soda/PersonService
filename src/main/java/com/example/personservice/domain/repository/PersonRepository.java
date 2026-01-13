package com.example.personservice.domain.repository;

import com.example.personservice.domain.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface PersonRepository extends JpaRepository<Person, Long> {
    Optional<Person> findByTaxNumber(String taxNumber);

    @Query("""
        SELECT p FROM Person p
        WHERE (
            LOWER(p.firstName) LIKE LOWER(CONCAT(:prefix, '%'))
            OR LOWER(p.lastName) LIKE LOWER(CONCAT(:prefix, '%'))
        )
        AND (YEAR(CURRENT_DATE) - YEAR(p.dateOfBirth)) > :minAge
    """)
    List<Person> findByNameAndAge(String prefix, int minAge);

    boolean existsByTaxNumber(String taxNumber);
}