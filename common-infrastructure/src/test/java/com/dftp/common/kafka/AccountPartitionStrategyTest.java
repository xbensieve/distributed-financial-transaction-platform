package com.dftp.common.kafka;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class AccountPartitionStrategyTest {

    @Test
    @DisplayName("Standard account should partition directly by account ID for causal ordering")
    void testStandardAccountPartitioning() {
        String accountId = UUID.randomUUID().toString();
        String txId = UUID.randomUUID().toString();

        String partitionKey1 = AccountPartitionStrategy.resolvePartitionKey(accountId, txId);
        String partitionKey2 = AccountPartitionStrategy.resolvePartitionKey(accountId, UUID.randomUUID().toString());

        // For standard account, partition key strictly matches account ID regardless of transaction ID
        assertThat(partitionKey1).isEqualTo(accountId);
        assertThat(partitionKey2).isEqualTo(accountId);
    }

    @Test
    @DisplayName("High-volume merchant accounts should apply deterministic bounded salt sub-partitioning")
    void testHotMerchantAccountPartitioning() {
        String merchantAccount = "MERCHANT-AMAZON-001";
        
        Set<String> observedKeys = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            String txId = "tx-" + i;
            String key = AccountPartitionStrategy.resolvePartitionKey(merchantAccount, txId);
            assertThat(key).startsWith("MERCHANT-AMAZON-001#");
            observedKeys.add(key);
        }

        // Bounded across up to 8 sub-buckets
        assertThat(observedKeys.size()).isGreaterThan(1).isLessThanOrEqualTo(AccountPartitionStrategy.DEFAULT_SKEW_BUCKETS);
    }

    @Test
    @DisplayName("Treasury and pool prefixes should trigger hot-partition mitigation")
    void testPoolPrefixes() {
        assertThat(AccountPartitionStrategy.isHotAccount("POOL-LIQUIDITY-EUR")).isTrue();
        assertThat(AccountPartitionStrategy.isHotAccount("TREASURY-VAULT-01")).isTrue();
        assertThat(AccountPartitionStrategy.isHotAccount("SETTLEMENT-SWIFT-BATCH")).isTrue();
        assertThat(AccountPartitionStrategy.isHotAccount("CLEARING-HOUSE-ACH")).isTrue();
        assertThat(AccountPartitionStrategy.isHotAccount("USER-REGULAR-12345")).isFalse();
    }

    @Test
    @DisplayName("Custom registered hot account should apply salt sub-partitioning")
    void testCustomRegisteredHotAccount() {
        String customHotAccount = "HIGH-TRAFFIC-PARTNER-XYZ";
        assertThat(AccountPartitionStrategy.isHotAccount(customHotAccount)).isFalse();

        AccountPartitionStrategy.registerHotAccount(customHotAccount);
        assertThat(AccountPartitionStrategy.isHotAccount(customHotAccount)).isTrue();

        String key = AccountPartitionStrategy.resolvePartitionKey(customHotAccount, "tx-999");
        assertThat(key).startsWith("HIGH-TRAFFIC-PARTNER-XYZ#");
    }

    @Test
    @DisplayName("Fallback for null or blank account ID should use transaction ID")
    void testNullAccountFallback() {
        String txId = "fallback-tx-123";
        assertThat(AccountPartitionStrategy.resolvePartitionKey(null, txId)).isEqualTo(txId);
        assertThat(AccountPartitionStrategy.resolvePartitionKey("", txId)).isEqualTo(txId);
    }
}
