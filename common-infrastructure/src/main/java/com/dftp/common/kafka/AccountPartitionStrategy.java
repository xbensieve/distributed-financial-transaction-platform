package com.dftp.common.kafka;

import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Account Partitioning Strategy (P13 / ADR-012).
 * Solves the dual challenge of:
 * 1. Per-Account Causal Ordering: Standard customer transactions partition by account ID
 *    to preserve strict FIFO sequence and eliminate optimistic locking collisions.
 * 2. Hot Partition Skew Mitigation: High-volume merchant/treasury/pool accounts employ
 *    bounded salt sub-partitioning to distribute bursts across multiple Kafka partitions.
 */
@Slf4j
public final class AccountPartitionStrategy {

    public static final int DEFAULT_SKEW_BUCKETS = 8;

    private static final Set<String> HOT_ACCOUNT_PREFIXES = Set.of(
            "MERCHANT", "POOL", "TREASURY", "SETTLEMENT", "CLEARING"
    );

    private static final Set<String> REGISTERED_HOT_ACCOUNTS = ConcurrentHashMap.newKeySet();

    private AccountPartitionStrategy() {
    }

    /**
     * Registers an account identifier as a hot/pool account.
     */
    public static void registerHotAccount(String accountId) {
        if (accountId != null && !accountId.isBlank()) {
            REGISTERED_HOT_ACCOUNTS.add(accountId.trim().toUpperCase());
        }
    }

    /**
     * Resolves the Kafka partition key for an account operation.
     *
     * @param accountId     The source or target account identifier (UUID or String)
     * @param transactionId The unique transaction identifier
     * @return Deterministic Kafka partition key
     */
    public static String resolvePartitionKey(String accountId, String transactionId) {
        if (accountId == null || accountId.isBlank()) {
            return transactionId != null ? transactionId : "unknown-partition-key";
        }

        String normalizedAcc = accountId.trim();
        if (isHotAccount(normalizedAcc)) {
            int saltBucket = transactionId != null 
                    ? Math.abs(transactionId.hashCode()) % DEFAULT_SKEW_BUCKETS 
                    : 0;
            String saltedKey = normalizedAcc + "#" + saltBucket;
            log.debug("Applying hot-partition salt: account {} -> partitionKey {}", normalizedAcc, saltedKey);
            return saltedKey;
        }

        return normalizedAcc;
    }

    /**
     * Resolves the Kafka partition key with explicit high-volume override flag.
     */
    public static String resolvePartitionKey(String accountId, String transactionId, boolean forceHotPartitionSalt) {
        if (forceHotPartitionSalt) {
            String normalizedAcc = accountId != null ? accountId.trim() : "pool";
            int saltBucket = transactionId != null 
                    ? Math.abs(transactionId.hashCode()) % DEFAULT_SKEW_BUCKETS 
                    : 0;
            return normalizedAcc + "#" + saltBucket;
        }
        return resolvePartitionKey(accountId, transactionId);
    }

    /**
     * Determines whether an account qualifies as a hot/pool account.
     */
    public static boolean isHotAccount(String accountId) {
        if (accountId == null) {
            return false;
        }
        String upper = accountId.toUpperCase();
        if (REGISTERED_HOT_ACCOUNTS.contains(upper)) {
            return true;
        }
        for (String prefix : HOT_ACCOUNT_PREFIXES) {
            if (upper.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }
}
