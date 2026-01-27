package com.example.personservice.domain.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
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
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tax {
    @Id
    @Column(name = "taxId", updatable = false, nullable = false)
    private String taxId;

    @Column(name = "taxDebt")
    private BigDecimal taxDebt;

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
