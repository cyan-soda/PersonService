package com.example.personservice.domain.model;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Table(name = "tax")
public class Tax {
    @Id
    @Column(name = "tax_number", updatable = false, nullable = false)
    private String taxNumber;

    @Column(name = "tax_debt", nullable = false)
    private BigDecimal taxDebt = BigDecimal.ZERO;

    @OneToOne(mappedBy = "taxInfo")
    private Person person;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

    public void addTaxDebt(BigDecimal amount) {
        if (amount != null && amount.compareTo(BigDecimal.ZERO) > 0) {
            this.taxDebt = this.taxDebt.add(amount);
        }
    }
}