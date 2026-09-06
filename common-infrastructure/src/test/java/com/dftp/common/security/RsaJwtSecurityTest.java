package com.dftp.common.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RsaJwtSecurityTest {

    private RsaKeyProvider rsaKeyProvider;
    private JwtProperties jwtProperties;
    private JwtDecoder rsaJwtDecoder;
    private SecurityConfig securityConfig;

    @BeforeEach
    void setUp() {
        rsaKeyProvider = new RsaKeyProvider();
        jwtProperties = new JwtProperties();
        jwtProperties.setIssuer("https://auth.dftp.bank");
        jwtProperties.setAudience("dftp-api");
        jwtProperties.setExpirationSeconds(3600);
        jwtProperties.setClockSkewSeconds(0); // Strict clock for testing

        securityConfig = new SecurityConfig(jwtProperties, null, null);
        this.rsaJwtDecoder = securityConfig.jwtDecoder(rsaKeyProvider);
    }

    @Test
    @DisplayName("RS256-01: Valid RS256 token signed by active RSA key is verified")
    void testValidRsaToken() {
        Instant now = Instant.now();
        String token = rsaKeyProvider.createSignedToken(
                "customer-12345",
                List.of("ROLE_USER"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plusSeconds(3600),
                rsaKeyProvider.getCurrentKeyId()
        );

        Jwt jwt = rsaJwtDecoder.decode(token);
        assertThat(jwt.getSubject()).isEqualTo("customer-12345");
        assertThat(jwt.getIssuer().toString()).isEqualTo("https://auth.dftp.bank");
        assertThat(jwt.getAudience()).contains("dftp-api");

        AbstractAuthenticationToken auth = securityConfig.jwtAuthenticationConverter().convert(jwt);
        assertThat(auth).isNotNull();
        List<String> authorities = auth.getAuthorities().stream().map(GrantedAuthority::getAuthority).toList();
        assertThat(authorities).containsExactly("ROLE_USER");
    }

    @Test
    @DisplayName("RS256-02: Expired RS256 token is rejected")
    void testExpiredRsaToken() {
        Instant past = Instant.now().minus(Duration.ofHours(2));
        String token = rsaKeyProvider.createSignedToken(
                "customer-expired",
                List.of("ROLE_USER"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                past,
                past.plusSeconds(1800), // Expired 1.5 hours ago
                rsaKeyProvider.getCurrentKeyId()
        );

        assertThatThrownBy(() -> rsaJwtDecoder.decode(token))
                .isInstanceOf(JwtException.class)
                .hasMessageContaining("Expired");
    }

    @Test
    @DisplayName("RS256-03: RS256 token with wrong issuer is rejected")
    void testWrongIssuerRsaToken() {
        Instant now = Instant.now();
        String token = rsaKeyProvider.createSignedToken(
                "customer-attacker",
                List.of("ROLE_ADMIN"),
                "https://untrusted-idp.attacker.org",
                jwtProperties.getAudience(),
                now,
                now.plusSeconds(3600),
                rsaKeyProvider.getCurrentKeyId()
        );

        assertThatThrownBy(() -> rsaJwtDecoder.decode(token))
                .isInstanceOf(JwtValidationException.class)
                .hasMessageContaining("iss");
    }

    @Test
    @DisplayName("RS256-04: RS256 token with wrong audience is rejected")
    void testWrongAudienceRsaToken() {
        Instant now = Instant.now();
        String token = rsaKeyProvider.createSignedToken(
                "customer-attacker",
                List.of("ROLE_ADMIN"),
                jwtProperties.getIssuer(),
                "wrong-service-audience",
                now,
                now.plusSeconds(3600),
                rsaKeyProvider.getCurrentKeyId()
        );

        assertThatThrownBy(() -> rsaJwtDecoder.decode(token))
                .isInstanceOf(JwtValidationException.class)
                .matches(e -> e.getMessage().toLowerCase().contains("audience"));
    }

    @Test
    @DisplayName("RS256-05: RS256 token with forged signature is rejected")
    void testForgedSignatureRsaToken() {
        Instant now = Instant.now();
        // Create an independent attacker keypair
        RsaKeyProvider attackerProvider = new RsaKeyProvider();
        attackerProvider.rotateKey("attacker-key-untrusted");
        String token = attackerProvider.createSignedToken(
                "forged-admin",
                List.of("ROLE_ADMIN"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plusSeconds(3600),
                attackerProvider.getCurrentKeyId()
        );

        // Verifier uses the legitimate DFTP JWKSet -> cannot verify attacker signature
        assertThatThrownBy(() -> rsaJwtDecoder.decode(token))
                .isInstanceOf(BadJwtException.class);
    }

    @Test
    @DisplayName("RS256-06: Key rotation allows verifying tokens signed by both new and rotated keys")
    void testKeyRotation() {
        String key1Id = rsaKeyProvider.getCurrentKeyId();
        Instant now = Instant.now();

        // 1. Sign token with Key 1
        String tokenKey1 = rsaKeyProvider.createSignedToken(
                "user-rotated-1",
                List.of("ROLE_USER"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plusSeconds(3600),
                key1Id
        );

        // 2. Rotate to Key 2
        String key2Id = rsaKeyProvider.rotateKey("rotated-key-2");
        String tokenKey2 = rsaKeyProvider.createSignedToken(
                "user-rotated-2",
                List.of("ROLE_USER"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plusSeconds(3600),
                key2Id
        );

        // 3. Both tokens must decode successfully because JWKS contains both keys
        assertThat(rsaJwtDecoder.decode(tokenKey1).getSubject()).isEqualTo("user-rotated-1");
        assertThat(rsaJwtDecoder.decode(tokenKey2).getSubject()).isEqualTo("user-rotated-2");

        // 4. Revoke Key 1 -> tokenKey1 must fail immediately
        rsaKeyProvider.revokeKey(key1Id);
        assertThatThrownBy(() -> rsaJwtDecoder.decode(tokenKey1))
                .isInstanceOf(BadJwtException.class);

        // Token signed with Key 2 still succeeds
        assertThat(rsaJwtDecoder.decode(tokenKey2).getSubject()).isEqualTo("user-rotated-2");
    }

    @Test
    @DisplayName("RS256-07: Token with missing subject claim is rejected")
    void testMissingSubjectRejected() {
        Instant now = Instant.now();
        String token = rsaKeyProvider.createSignedToken(
                null, // Missing subject
                List.of("ROLE_USER"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plusSeconds(3600),
                rsaKeyProvider.getCurrentKeyId()
        );

        assertThatThrownBy(() -> rsaJwtDecoder.decode(token))
                .isInstanceOf(JwtValidationException.class)
                .hasMessageContaining("subject");
    }
}
