package com.dftp.common.kafka;

import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.*;

/**
 * Authoritative Least-Privilege Kafka Security Policy Engine for DFTP.
 * Enforces topic, consumer-group, and role-based ACL permissions across services.
 */
public class KafkaSecurityPolicy {

    public static final String PRINCIPAL_ACCOUNT_SERVICE = "User:account-service";
    public static final String PRINCIPAL_TRANSACTION_SERVICE = "User:transaction-service";
    public static final String PRINCIPAL_LEDGER_SERVICE = "User:ledger-service";
    public static final String PRINCIPAL_ADMIN_OPERATOR = "User:dlt-operator";

    public static final String TOPIC_ACCOUNT_EVENTS = "account-events";
    public static final String TOPIC_TRANSACTION_EVENTS = "transaction-events";
    public static final String TOPIC_LEDGER_EVENTS = "ledger-events";

    private static final Map<String, Set<String>> PRODUCER_PERMISSIONS = Map.of(
            PRINCIPAL_ACCOUNT_SERVICE, Set.of(TOPIC_ACCOUNT_EVENTS, TOPIC_ACCOUNT_EVENTS + ".DLT"),
            PRINCIPAL_TRANSACTION_SERVICE, Set.of(TOPIC_TRANSACTION_EVENTS, TOPIC_TRANSACTION_EVENTS + ".DLT"),
            PRINCIPAL_LEDGER_SERVICE, Set.of(TOPIC_LEDGER_EVENTS, TOPIC_LEDGER_EVENTS + ".DLT"),
            PRINCIPAL_ADMIN_OPERATOR, Set.of(TOPIC_ACCOUNT_EVENTS, TOPIC_TRANSACTION_EVENTS, TOPIC_LEDGER_EVENTS)
    );

    private static final Map<String, Set<String>> CONSUMER_TOPIC_PERMISSIONS = Map.of(
            PRINCIPAL_ACCOUNT_SERVICE, Set.of(TOPIC_TRANSACTION_EVENTS),
            PRINCIPAL_TRANSACTION_SERVICE, Set.of(TOPIC_ACCOUNT_EVENTS, TOPIC_LEDGER_EVENTS),
            PRINCIPAL_LEDGER_SERVICE, Set.of(TOPIC_TRANSACTION_EVENTS),
            PRINCIPAL_ADMIN_OPERATOR, Set.of(TOPIC_ACCOUNT_EVENTS + ".DLT", TOPIC_TRANSACTION_EVENTS + ".DLT", TOPIC_LEDGER_EVENTS + ".DLT")
    );

    private static final Map<String, Set<String>> CONSUMER_GROUP_PERMISSIONS = Map.of(
            PRINCIPAL_ACCOUNT_SERVICE, Set.of("account-service-group"),
            PRINCIPAL_TRANSACTION_SERVICE, Set.of("transaction-service-saga-group"),
            PRINCIPAL_LEDGER_SERVICE, Set.of("ledger-service-group"),
            PRINCIPAL_ADMIN_OPERATOR, Set.of("dlt-replay-group")
    );

    /**
     * Evaluates whether a principal is authorized to produce to a specific topic.
     */
    public boolean canProduce(String principal, String topic) {
        Set<String> allowedTopics = PRODUCER_PERMISSIONS.get(principal);
        return allowedTopics != null && allowedTopics.contains(topic);
    }

    /**
     * Evaluates whether a principal is authorized to consume from a topic using a consumer group.
     */
    public boolean canConsume(String principal, String topic, String consumerGroup) {
        Set<String> allowedTopics = CONSUMER_TOPIC_PERMISSIONS.get(principal);
        Set<String> allowedGroups = CONSUMER_GROUP_PERMISSIONS.get(principal);

        boolean topicAllowed = allowedTopics != null && allowedTopics.contains(topic);
        boolean groupAllowed = allowedGroups != null && allowedGroups.contains(consumerGroup);

        return topicAllowed && groupAllowed;
    }

    /**
     * Validates that Kafka client properties comply with production TLS 1.3 and SASL/SCRAM standards.
     */
    public void validateProductionProperties(Properties props) {
        String protocol = props.getProperty("security.protocol");
        if (protocol == null || !protocol.equals(SecurityProtocol.SASL_SSL.name)) {
            throw new IllegalArgumentException("Insecure Kafka protocol: production requires SASL_SSL, found: " + protocol);
        }

        String saslMechanism = props.getProperty("sasl.mechanism");
        if (saslMechanism == null || !saslMechanism.equals("SCRAM-SHA-512")) {
            throw new IllegalArgumentException("Insecure SASL mechanism: production requires SCRAM-SHA-512, found: " + saslMechanism);
        }

        String sslProtocol = props.getProperty("ssl.protocol");
        if (sslProtocol == null || !sslProtocol.equals("TLSv1.3")) {
            throw new IllegalArgumentException("Insecure TLS protocol: production requires TLSv1.3, found: " + sslProtocol);
        }

        String truststoreType = props.getProperty("ssl.truststore.type");
        if (truststoreType == null || !truststoreType.equalsIgnoreCase("PKCS12")) {
            throw new IllegalArgumentException("Production requires PKCS12 truststore format, found: " + truststoreType);
        }
    }
}
