package com.dftp.transaction.security;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "security_audit_logs")
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class SecurityAuditLog {

    @Id
    private UUID id;

    @Column(nullable = false)
    private String principal;

    private String roles;

    @Column(nullable = false)
    private String action;

    @Column(name = "target_resource", nullable = false)
    private String targetResource;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(columnDefinition = "jsonb")
    private String details;

    @Column(nullable = false)
    private String status;

    @Column(name = "client_ip")
    private String clientIp;

    @Column(name = "correlation_id")
    private String correlationId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;
}
