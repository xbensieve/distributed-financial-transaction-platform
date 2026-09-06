package com.dftp.transaction.domain;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.*;

import java.time.Instant;

@Entity
@Table(name = "api_idempotency_keys")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ApiIdempotencyKey {

    @Id
    private String idempotencyKey;

    private String requestHash;

    private Integer responseStatus;

    // We store the serialized response body JSON
    @jakarta.persistence.Column(columnDefinition = "jsonb")
    @org.hibernate.annotations.JdbcTypeCode(org.hibernate.type.SqlTypes.JSON)
    private String responseBody;

    private Instant createdAt;
}
