package com.dftp.account.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface AccountOperationRepository extends JpaRepository<AccountOperation, UUID> {
    List<AccountOperation> findByTransactionId(String transactionId);

    List<AccountOperation> findByAccountId(UUID accountId);

    Optional<AccountOperation> findByTransactionIdAndOperationTypeAndAccountId(String transactionId, String operationType, UUID accountId);

    List<AccountOperation> findByTransactionIdIn(Collection<String> transactionIds);

    @Query("SELECT o FROM AccountOperation o WHERE o.operationType = 'HOLD' AND o.createdAt < :cutoff " +
           "AND NOT EXISTS (SELECT 1 FROM AccountOperation o2 WHERE o2.transactionId = o.transactionId AND o2.operationType IN ('SETTLE', 'COMPENSATE')) " +
           "ORDER BY o.createdAt ASC, o.id ASC")
    List<AccountOperation> findOrphanHoldCandidates(@Param("cutoff") Instant cutoff, Pageable pageable);

    @Modifying
    @Transactional
    @Query(value = "INSERT INTO account_operations (id, account_id, transaction_id, operation_type) " +
                   "VALUES (:id, :accountId, :transactionId, :operationType) " +
                   "ON CONFLICT (transaction_id, operation_type, account_id) DO NOTHING", nativeQuery = true)
    int insertIfAbsent(UUID id, UUID accountId, String transactionId, String operationType);
}
