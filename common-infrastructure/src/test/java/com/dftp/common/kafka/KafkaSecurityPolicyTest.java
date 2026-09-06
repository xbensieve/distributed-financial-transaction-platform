package com.dftp.common.kafka;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KafkaSecurityPolicyTest {

    private KafkaSecurityPolicy policy;

    @BeforeEach
    void setUp() {
        policy = new KafkaSecurityPolicy();
    }

    @Test
    @DisplayName("KAFKA-SEC-01: Account service can publish only account events, cannot forge transaction or ledger events")
    void testAccountServiceProducerAcls() {
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_ACCOUNT_SERVICE, KafkaSecurityPolicy.TOPIC_ACCOUNT_EVENTS)).isTrue();
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_ACCOUNT_SERVICE, "account-events.DLT")).isTrue();

        // Must NOT be able to produce transaction or ledger events
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_ACCOUNT_SERVICE, KafkaSecurityPolicy.TOPIC_TRANSACTION_EVENTS)).isFalse();
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_ACCOUNT_SERVICE, KafkaSecurityPolicy.TOPIC_LEDGER_EVENTS)).isFalse();
    }

    @Test
    @DisplayName("KAFKA-SEC-02: Transaction service can publish transaction events, cannot publish ledger events")
    void testTransactionServiceProducerAcls() {
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_TRANSACTION_SERVICE, KafkaSecurityPolicy.TOPIC_TRANSACTION_EVENTS)).isTrue();
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_TRANSACTION_SERVICE, "transaction-events.DLT")).isTrue();

        // Transaction service cannot forge ledger postings!
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_TRANSACTION_SERVICE, KafkaSecurityPolicy.TOPIC_LEDGER_EVENTS)).isFalse();
        assertThat(policy.canProduce(KafkaSecurityPolicy.PRINCIPAL_TRANSACTION_SERVICE, KafkaSecurityPolicy.TOPIC_ACCOUNT_EVENTS)).isFalse();
    }

    @Test
    @DisplayName("KAFKA-SEC-03: Ledger service consumer permissions — cannot consume unauthorized business topics")
    void testLedgerServiceConsumerAcls() {
        // Can consume transaction events with its designated consumer group
        assertThat(policy.canConsume(KafkaSecurityPolicy.PRINCIPAL_LEDGER_SERVICE,
                KafkaSecurityPolicy.TOPIC_TRANSACTION_EVENTS, "ledger-service-group")).isTrue();

        // Cannot consume account-events
        assertThat(policy.canConsume(KafkaSecurityPolicy.PRINCIPAL_LEDGER_SERVICE,
                KafkaSecurityPolicy.TOPIC_ACCOUNT_EVENTS, "ledger-service-group")).isFalse();

        // Cannot hijack other service groups
        assertThat(policy.canConsume(KafkaSecurityPolicy.PRINCIPAL_LEDGER_SERVICE,
                KafkaSecurityPolicy.TOPIC_TRANSACTION_EVENTS, "transaction-service-saga-group")).isFalse();
    }

    @Test
    @DisplayName("KAFKA-SEC-04: Unauthorized client is completely blocked from producing or consuming")
    void testUnauthorizedClientBlocked() {
        String attacker = "User:rogue-client";

        assertThat(policy.canProduce(attacker, KafkaSecurityPolicy.TOPIC_TRANSACTION_EVENTS)).isFalse();
        assertThat(policy.canProduce(attacker, KafkaSecurityPolicy.TOPIC_ACCOUNT_EVENTS)).isFalse();
        assertThat(policy.canProduce(attacker, KafkaSecurityPolicy.TOPIC_LEDGER_EVENTS)).isFalse();

        assertThat(policy.canConsume(attacker, KafkaSecurityPolicy.TOPIC_TRANSACTION_EVENTS, "rogue-group")).isFalse();
        assertThat(policy.canConsume(attacker, "transaction-events.DLT", "rogue-group")).isFalse();
    }

    @Test
    @DisplayName("KAFKA-SEC-05: DLT consumption is restricted exclusively to authorized DLT operator")
    void testDltConsumptionRestricted() {
        // Regular services cannot consume DLTs directly
        assertThat(policy.canConsume(KafkaSecurityPolicy.PRINCIPAL_TRANSACTION_SERVICE, "transaction-events.DLT", "transaction-service-saga-group")).isFalse();
        assertThat(policy.canConsume(KafkaSecurityPolicy.PRINCIPAL_ACCOUNT_SERVICE, "account-events.DLT", "account-service-group")).isFalse();

        // Only authorized DLT operator can consume DLT
        assertThat(policy.canConsume(KafkaSecurityPolicy.PRINCIPAL_ADMIN_OPERATOR, "transaction-events.DLT", "dlt-replay-group")).isTrue();
    }

    @Test
    @DisplayName("KAFKA-SEC-06: Production properties validation enforces SASL_SSL, SCRAM-SHA-512, and TLS 1.3")
    void testProductionPropertiesValidation() {
        Properties secureProps = new Properties();
        secureProps.setProperty("security.protocol", "SASL_SSL");
        secureProps.setProperty("sasl.mechanism", "SCRAM-SHA-512");
        secureProps.setProperty("ssl.protocol", "TLSv1.3");
        secureProps.setProperty("ssl.truststore.type", "PKCS12");

        // Should pass without exception
        policy.validateProductionProperties(secureProps);

        // Should reject PLAINTEXT
        Properties insecureProps = new Properties();
        insecureProps.setProperty("security.protocol", "PLAINTEXT");
        assertThatThrownBy(() -> policy.validateProductionProperties(insecureProps))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Insecure Kafka protocol");
    }
}
