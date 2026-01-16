package com.example.personservice.infrastructure.repository;

import com.example.personservice.domain.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface PersonRepository extends JpaRepository<Person, UUID> {
    Optional<Person> findByTaxNumber(String taxNumber);

    // todo: minAge is not required, can  search for each param independently
    // tai sao khong nen dung @Query, co the dung JPA, tai sao nen uu tien JPA
    @Query("""
        SELECT p FROM Person p
        WHERE (
            LOWER(p.firstName) LIKE LOWER(CONCAT(:firstNamePrefix, '%'))
            OR LOWER(p.lastName) LIKE LOWER(CONCAT(:lastNamePrefix, '%'))
        )
        AND (YEAR(CURRENT_DATE) - YEAR(p.dateOfBirth)) > :minAge
    """)
    List<Person> findByNameAndAge(String firstNamePrefix, String lastNamePrefix, int minAge);

    boolean existsByTaxNumber(String taxNumber);
}