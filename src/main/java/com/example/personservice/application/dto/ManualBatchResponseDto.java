package com.example.personservice.application.dto;

import java.util.List;

public record ManualBatchResponseDto (
    List<String> events,
    long messagesLeft,
    boolean hasMoreMessages
) {}
