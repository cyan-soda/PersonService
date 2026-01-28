package com.example.personservice.infrastructure.repository;

import com.example.personservice.domain.model.Person;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface PersonRepository extends JpaRepository<Person, UUID>, JpaSpecificationExecutor<Person>{
    Optional<Person> findByTaxInfo_TaxNumber(String taxNumber);
}