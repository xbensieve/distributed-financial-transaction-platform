package com.dftp.transaction.security;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface SecurityAuditLogRepository extends JpaRepository<SecurityAuditLog, UUID> {
    List<SecurityAuditLog> findByActionOrderByCreatedAtDesc(String action);
    List<SecurityAuditLog> findByTargetResourceOrderByCreatedAtDesc(String targetResource);

    @org.springframework.data.jpa.repository.Modifying
    @org.springframework.data.jpa.repository.Query(value = "WITH to_delete AS (" +
                   "  SELECT id FROM security_audit_logs " +
                   "  WHERE created_at < :cutoff " +
                   "  LIMIT :limit" +
                   ") " +
                   "DELETE FROM security_audit_logs " +
                   "WHERE id IN (SELECT id FROM to_delete)", nativeQuery = true)
    int deleteOldLogsChunk(@org.springframework.data.repository.query.Param("cutoff") java.time.Instant cutoff,
                           @org.springframework.data.repository.query.Param("limit") int limit);
}
