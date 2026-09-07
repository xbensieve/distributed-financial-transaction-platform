package com.dftp.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * Encapsulates cross-service snapshot lookup results.
 * Distinguishes confirmed non-existence (NOT_FOUND) from network timeouts or dependency outages (DEPENDENCY_UNAVAILABLE).
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SnapshotResult<T> implements Serializable {

    public enum Status {
        FOUND,
        NOT_FOUND,
        DEPENDENCY_UNAVAILABLE
    }

    private Status status;
    private T data;
    private String errorMessage;
    private Integer statusCode;

    public boolean isFound() {
        return Status.FOUND.equals(status);
    }

    public boolean isNotFound() {
        return Status.NOT_FOUND.equals(status);
    }

    public boolean isUnavailable() {
        return Status.DEPENDENCY_UNAVAILABLE.equals(status);
    }

    public static <T> SnapshotResult<T> found(T data) {
        return SnapshotResult.<T>builder()
                .status(Status.FOUND)
                .data(data)
                .statusCode(200)
                .build();
    }

    public static <T> SnapshotResult<T> notFound(String message) {
        return SnapshotResult.<T>builder()
                .status(Status.NOT_FOUND)
                .errorMessage(message)
                .statusCode(404)
                .build();
    }

    public static <T> SnapshotResult<T> unavailable(String message, Integer statusCode) {
        return SnapshotResult.<T>builder()
                .status(Status.DEPENDENCY_UNAVAILABLE)
                .errorMessage(message)
                .statusCode(statusCode)
                .build();
    }

    public static <T> SnapshotResult<T> unavailable(String message) {
        return unavailable(message, 503);
    }
}
