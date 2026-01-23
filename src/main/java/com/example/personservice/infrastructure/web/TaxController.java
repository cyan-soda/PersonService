package com.example.personservice.infrastructure.web;

import com.example.personservice.application.dto.person.OperationResponseDto;
import com.example.personservice.application.dto.tax.TaxRequestDto;
import com.example.personservice.application.dto.tax.TaxResponseDto;
import com.example.personservice.application.service.TaxService;
import com.example.personservice.infrastructure.validation.validator.ValidTaxNumber;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;

@Slf4j
@RestController
@RequestMapping("/tax")
@Validated
public class TaxController {
    private final TaxService service;

    public TaxController(TaxService service) {
        this.service = service;
    }

    @GetMapping("/debt/{taxNumber}")
    public ResponseEntity<TaxResponseDto> getTaxDebt(@PathVariable @ValidTaxNumber String taxNumber) {
        TaxResponseDto response = service.getTaxDebt(taxNumber);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/debt/{taxNumber}")
    public ResponseEntity<OperationResponseDto> handleTaxCalculation(
            @PathVariable @ValidTaxNumber String taxNumber,
            @RequestBody @Valid TaxRequestDto request
    ) {
        OperationResponseDto response = service.handleTaxCalculation(taxNumber, request.getAmount());
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
