package com.dftp.ledger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.DockerClientFactory;

import static org.assertj.core.api.Assertions.assertThat;

class CiValidationTest {

    @Test
    @EnabledIfSystemProperty(named = "ci", matches = "true")
    void dockerMustBeAvailableInCi() {
        // If the CI profile is active (-Pci), this test runs and ensures Testcontainers
        // hasn't been silently skipped due to Docker missing.
        assertThat(DockerClientFactory.instance().isDockerAvailable())
                .as("Docker MUST be available in the CI environment to run integration tests.")
                .isTrue();
    }
}
