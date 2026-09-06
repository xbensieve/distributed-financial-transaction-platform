package com.dftp.account.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;
import java.util.UUID;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

public interface AccountOperationRepository extends JpaRepository<AccountOperation, UUID> {
    Optional<AccountOperation> findByTransactionIdAndOperationTypeAndAccountId(String transactionId, String operationType, UUID accountId);

    @Modifying
    @Transactional
    @Query(value = "INSERT INTO account_operations (id, account_id, transaction_id, operation_type) " +
                   "VALUES (:id, :accountId, :transactionId, :operationType) " +
                   "ON CONFLICT (transaction_id, operation_type, account_id) DO NOTHING", nativeQuery = true)
    int insertIfAbsent(UUID id, UUID accountId, String transactionId, String operationType);
}
