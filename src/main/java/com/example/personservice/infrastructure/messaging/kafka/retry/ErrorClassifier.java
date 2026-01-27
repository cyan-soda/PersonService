package com.example.personservice.infrastructure.messaging.kafka.retry;

import com.example.personservice.infrastructure.exception.PersonNotFoundException;
import com.example.personservice.infrastructure.exception.PersonAlreadyExistsException;
import com.example.personservice.infrastructure.exception.KafkaConsumerException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.TimeoutException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;

@Slf4j
@Component
public class ErrorClassifier {

    public enum ErrorType { FATAL, RETRYABLE }

    public ErrorType classifyError(Exception exception) {
        Throwable cause = exception;
        while ((cause instanceof KafkaConsumerException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }

        log.debug("Classifying error: {} (Original: {})", cause.getClass().getSimpleName(), exception.getClass().getSimpleName());

        if (isFatal(cause)) {
            return ErrorType.FATAL;
        }

        if (isRetryable(cause)) {
            return ErrorType.RETRYABLE;
        }

        return ErrorType.RETRYABLE;
    }

    private boolean isFatal(Throwable t) {
        return t instanceof JsonProcessingException ||
                t instanceof IllegalArgumentException ||
                t instanceof PersonNotFoundException ||      // Logic error: ID doesn't exist
                t instanceof PersonAlreadyExistsException || // Logic error: ID already exists
                t instanceof DataIntegrityViolationException; // DB Constraint (not null, etc)
    }

    private boolean isRetryable(Throwable t) {
        return t instanceof SQLException ||
                t instanceof ConnectException ||
                t instanceof SocketTimeoutException ||
                t instanceof TimeoutException ||
                t instanceof TransientDataAccessException ||
                t instanceof RecoverableDataAccessException;
    }
}
