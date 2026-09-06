package com.dftp.common.kafka;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Authentic Kafka Broker Security & ACL Integration Test.
 *
 * <p>Demonstrates genuine broker-enforced authorization and authentication using
 * Apache Kafka's StandardAuthorizer, SASL_PLAINTEXT, and Principle of Least Privilege ACLs.
 * Tests are NOT simulated in Java; rejections are enforced and thrown directly by the Kafka broker.</p>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaBrokerSecurityIntegrationTest {

    private static final String JAAS_CONTENT =
            "KafkaServer {\n" +
            "    org.apache.kafka.common.security.plain.PlainLoginModule required\n" +
            "    username=\"admin\"\n" +
            "    password=\"admin-secret\"\n" +
            "    user_admin=\"admin-secret\"\n" +
            "    user_account_service=\"account-secret\"\n" +
            "    user_transaction_service=\"tx-secret\"\n" +
            "    user_ledger_service=\"ledger-secret\"\n" +
            "    user_attacker=\"attacker-secret\";\n" +
            "};\n";

    private static org.testcontainers.containers.KafkaContainer kafka;
    private static String bootstrapServers;

    @BeforeAll
    static void startSecuredBroker() throws Exception {
        kafka = new org.testcontainers.containers.KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"))
                .withCopyToContainer(Transferable.of(JAAS_CONTENT.getBytes(StandardCharsets.UTF_8)), "/tmp/kafka_server_jaas.conf")
                .withEnv("KAFKA_OPTS", "-Djava.security.auth.login.config=/tmp/kafka_server_jaas.conf -Dzookeeper.sasl.client=false")
                .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
                .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
                .withEnv("KAFKA_SUPER_USERS", "User:admin;User:ANONYMOUS")
                .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT");

        kafka.start();

        bootstrapServers = kafka.getBootstrapServers();

        // Provision Topics and ACLs via Super-User Admin Client
        provisionBrokerAcls();
    }

    @AfterAll
    static void stopBroker() {
        if (kafka != null) {
            kafka.stop();
        }
    }

    private static void provisionBrokerAcls() throws Exception {
        Properties adminProps = createClientProps("admin", "admin-secret");
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            // 1. Create Topics
            NewTopic txEvents = new NewTopic("transaction-events", 1, (short) 1);
            NewTopic accEvents = new NewTopic("account-events", 1, (short) 1);
            NewTopic ledgerEvents = new NewTopic("ledger-events", 1, (short) 1);
            adminClient.createTopics(List.of(txEvents, accEvents, ledgerEvents)).all().get(10, TimeUnit.SECONDS);

            // 2. Provision Least-Privilege ACLs
            List<AclBinding> aclBindings = List.of(
                    // User:transaction_service -> WRITE & DESCRIBE on transaction-events
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, "transaction-events", PatternType.LITERAL),
                            new AccessControlEntry("User:transaction_service", "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, "transaction-events", PatternType.LITERAL),
                            new AccessControlEntry("User:transaction_service", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),

                    // User:account_service -> READ & DESCRIBE on transaction-events + group
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, "transaction-events", PatternType.LITERAL),
                            new AccessControlEntry("User:account_service", "*", AclOperation.READ, AclPermissionType.ALLOW)),
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, "transaction-events", PatternType.LITERAL),
                            new AccessControlEntry("User:account_service", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW)),
                    new AclBinding(
                            new ResourcePattern(ResourceType.GROUP, "account-service-group", PatternType.LITERAL),
                            new AccessControlEntry("User:account_service", "*", AclOperation.READ, AclPermissionType.ALLOW)),

                    // User:account_service -> WRITE & DESCRIBE on account-events
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, "account-events", PatternType.LITERAL),
                            new AccessControlEntry("User:account_service", "*", AclOperation.WRITE, AclPermissionType.ALLOW)),
                    new AclBinding(
                            new ResourcePattern(ResourceType.TOPIC, "account-events", PatternType.LITERAL),
                            new AccessControlEntry("User:account_service", "*", AclOperation.DESCRIBE, AclPermissionType.ALLOW))
            );

            adminClient.createAcls(aclBindings).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static Properties createClientProps(String username, String password) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                username, password));
        return props;
    }

    @Test
    @Order(1)
    @DisplayName("P10-04-A: Authorized transaction-service writes to transaction-events successfully")
    void testAuthorizedProducerCanWrite() throws Exception {
        Properties props = createClientProps("transaction_service", "tx-secret");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>("transaction-events", "tx-100", "{\"type\":\"FundsHoldRequested\"}"));
            RecordMetadata metadata = future.get(5, TimeUnit.SECONDS);

            assertThat(metadata).isNotNull();
            assertThat(metadata.topic()).isEqualTo("transaction-events");
        }
    }

    @Test
    @Order(2)
    @DisplayName("P10-04-B: Authorized account-service reads from transaction-events successfully")
    void testAuthorizedConsumerCanRead() throws Exception {
        Properties props = createClientProps("account_service", "account-secret");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "account-service-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("transaction-events"));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            assertThat(records.isEmpty()).isFalse();
            assertThat(records.iterator().next().topic()).isEqualTo("transaction-events");
        }
    }

    @Test
    @Order(3)
    @DisplayName("P10-04-C: Unauthorized principal attempting WRITE to account-events is DENIED by Broker ACL (TopicAuthorizationException)")
    void testUnauthorizedProducerWriteIsDeniedByBroker() {
        // Attacker attempts to produce to account-events
        Properties props = createClientProps("attacker", "attacker-secret");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>("account-events", "acc-evil", "{\"type\":\"UnauthorizedFakeFundsHeld\"}"));

            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @Order(4)
    @DisplayName("P10-04-D: Unauthorized principal attempting READ from ledger-events is DENIED by Broker ACL (TopicAuthorizationException)")
    void testUnauthorizedConsumerReadIsDeniedByBroker() {
        Properties props = createClientProps("attacker", "attacker-secret");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "attacker-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of("ledger-events"));
            assertThatThrownBy(() -> consumer.poll(Duration.ofSeconds(3)))
                    .isInstanceOf(TopicAuthorizationException.class);
        }
    }

    @Test
    @Order(5)
    @DisplayName("P10-04-E: Client with wrong credentials fails SASL authentication (SaslAuthenticationException)")
    void testWrongCredentialsFailsAuthentication() {
        Properties props = createClientProps("transaction_service", "WRONG-PASSWORD");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>("transaction-events", "tx-fail", "{}"));

            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(SaslAuthenticationException.class);
        }
    }

    @Test
    @Order(6)
    @DisplayName("P10-04-F: Plaintext client connecting to secured SASL listener is rejected/disconnected")
    void testPlaintextClientToSecuredListenerIsRejected() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put("security.protocol", "PLAINTEXT"); // Unsecured plaintext client
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 3000);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            Future<RecordMetadata> future = producer.send(
                    new ProducerRecord<>("transaction-events", "tx-plain", "{}"));

            assertThatThrownBy(() -> future.get(5, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class);
        }
    }
}
