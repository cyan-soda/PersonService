package com.example.personservice.application.dto.person;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class OperationResponseDto {
    private String message;
    private String operationType;

    public OperationResponseDto(String message, String operationType) {
        this.message = message;
        this.operationType = operationType;
    }
}
