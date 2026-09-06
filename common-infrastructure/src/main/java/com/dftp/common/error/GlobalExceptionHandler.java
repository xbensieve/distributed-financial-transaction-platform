package com.dftp.common.error;

import io.micrometer.tracing.Tracer;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
@RequiredArgsConstructor
public class GlobalExceptionHandler {

    private final Tracer tracer;

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidationExceptions(MethodArgumentNotValidException ex, HttpServletRequest request) {
        List<ApiError.ValidationError> validationErrors = ex.getBindingResult().getAllErrors().stream()
                .map(error -> new ApiError.ValidationError(
                        ((FieldError) error).getField(),
                        error.getDefaultMessage()))
                .collect(Collectors.toList());

        ApiError apiError = ApiError.builder()
                .errorCode("VALIDATION_ERROR")
                .message("Request validation failed")
                .path(request.getRequestURI())
                .correlationId(getTraceId())
                .validationErrors(validationErrors)
                .build();

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(apiError);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ApiError> handleIllegalArgumentException(IllegalArgumentException ex, HttpServletRequest request) {
        log.warn("Illegal argument in request {}: {}", request.getRequestURI(), ex.getMessage());

        ApiError apiError = ApiError.builder()
                .errorCode("BAD_REQUEST")
                .message(ex.getMessage())
                .path(request.getRequestURI())
                .correlationId(getTraceId())
                .build();

        return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(apiError);
    }

    @ExceptionHandler(org.springframework.web.server.ResponseStatusException.class)
    public ResponseEntity<ApiError> handleResponseStatusException(org.springframework.web.server.ResponseStatusException ex, HttpServletRequest request) {
        log.warn("Response status exception in request {}: {}", request.getRequestURI(), ex.getMessage());

        ApiError apiError = ApiError.builder()
                .errorCode(ex.getStatusCode().toString())
                .message(ex.getReason() != null ? ex.getReason() : ex.getMessage())
                .path(request.getRequestURI())
                .correlationId(getTraceId())
                .build();

        return ResponseEntity.status(ex.getStatusCode()).body(apiError);
    }

    @ExceptionHandler(org.springframework.security.access.AccessDeniedException.class)
    public ResponseEntity<ApiError> handleAccessDeniedException(org.springframework.security.access.AccessDeniedException ex, HttpServletRequest request) {
        log.warn("Access denied for request {}: {}", request.getRequestURI(), ex.getMessage());

        ApiError apiError = ApiError.builder()
                .errorCode("FORBIDDEN")
                .message("Access denied: insufficient privileges")
                .path(request.getRequestURI())
                .correlationId(getTraceId())
                .build();

        return ResponseEntity.status(HttpStatus.FORBIDDEN).body(apiError);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleGenericException(Exception ex, HttpServletRequest request) {
        log.error("Unhandled exception processing request {}: {}", request.getRequestURI(), ex.getMessage(), ex);

        ApiError apiError = ApiError.builder()
                .errorCode("INTERNAL_SERVER_ERROR")
                .message("An unexpected internal server error occurred")
                .path(request.getRequestURI())
                .correlationId(getTraceId())
                .build();

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(apiError);
    }

    private String getTraceId() {
        if (tracer.currentSpan() != null) {
            return tracer.currentSpan().context().traceId();
        }
        return null;
    }
}
