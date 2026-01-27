package com.example.personservice.infrastructure.web;

import com.example.personservice.application.dto.ManualBatchResponseDto;
import com.example.personservice.infrastructure.messaging.kafka.TaxManualService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/tax/manual")
public class TaxManualController {

    private final TaxManualService service;

    @GetMapping("/poll")
    public ManualBatchResponseDto pollEvents(@RequestParam(defaultValue = "10") int maxSize) {
        return service.fetchBatch(maxSize);
    }
}
