package com.dftp.transaction.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SecurityAuditService {

    private final SecurityAuditLogRepository securityAuditLogRepository;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public SecurityAuditLog recordEvent(
            String principal,
            String roles,
            String action,
            String targetResource,
            String detailsJson,
            String status,
            String clientIp,
            String correlationId) {

        SecurityAuditLog auditLog = SecurityAuditLog.builder()
                .id(UUID.randomUUID())
                .principal(principal != null ? principal : "ANONYMOUS")
                .roles(roles)
                .action(action)
                .targetResource(targetResource)
                .details(detailsJson)
                .status(status)
                .clientIp(clientIp)
                .correlationId(correlationId)
                .createdAt(Instant.now())
                .build();

        SecurityAuditLog saved = securityAuditLogRepository.saveAndFlush(auditLog);
        log.info("SECURITY_AUDIT: action='{}' principal='{}' target='{}' status='{}' correlationId='{}'",
                action, principal, targetResource, status, correlationId);
        return saved;
    }
}
