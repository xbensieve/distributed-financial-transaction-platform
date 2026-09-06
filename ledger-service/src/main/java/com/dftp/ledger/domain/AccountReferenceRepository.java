package com.dftp.ledger.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface AccountReferenceRepository extends JpaRepository<AccountReference, UUID> {
    Optional<AccountReference> findByAccountId(UUID accountId);

    @Modifying
    @Query(value = "INSERT INTO account_references (id, account_id, status, created_at, updated_at) " +
                   "VALUES (:id, :accountId, :status, NOW(), NOW()) " +
                   "ON CONFLICT (account_id) DO NOTHING", nativeQuery = true)
    int insertIfAbsent(@Param("id") UUID id, @Param("accountId") UUID accountId, @Param("status") String status);
}
