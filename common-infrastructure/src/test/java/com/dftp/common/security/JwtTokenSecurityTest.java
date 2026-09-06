package com.dftp.common.security;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtValidationException;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JwtTokenSecurityTest {

    private JwtProperties jwtProperties;
    private RsaKeyProvider rsaKeyProvider;
    private JwtTokenService jwtTokenService;
    private JwtDecoder jwtDecoder;
    private SecurityConfig securityConfig;

    @BeforeEach
    void setUp() {
        jwtProperties = new JwtProperties();
        jwtProperties.setSecret("dftp-dev-secret-key-must-be-at-least-32-bytes-long-for-hmac256");
        jwtProperties.setIssuer("https://auth.dftp.bank");
        jwtProperties.setAudience("dftp-api");
        jwtProperties.setExpirationSeconds(3600);
        jwtProperties.setClockSkewSeconds(0); // 0 skew for strict test validation

        rsaKeyProvider = new RsaKeyProvider();
        jwtTokenService = new JwtTokenService(jwtProperties, rsaKeyProvider);
        securityConfig = new SecurityConfig(jwtProperties, null, null);
        jwtDecoder = securityConfig.jwtDecoder(rsaKeyProvider);
    }

    @Test
    @DisplayName("SEC-01: Valid RS256 JWT token is successfully decoded and verified")
    void testValidToken() {
        String token = jwtTokenService.generateToken("user-123", List.of("ROLE_USER", "ROLE_ADMIN"));
        Jwt jwt = jwtDecoder.decode(token);

        assertThat(jwt.getSubject()).isEqualTo("user-123");
        assertThat(jwt.getIssuer().toString()).isEqualTo("https://auth.dftp.bank");
        assertThat(jwt.getAudience()).contains("dftp-api");

        AbstractAuthenticationToken auth = securityConfig.jwtAuthenticationConverter().convert(jwt);
        assertThat(auth).isNotNull();
        List<String> authorities = auth.getAuthorities().stream().map(GrantedAuthority::getAuthority).toList();
        assertThat(authorities).containsExactlyInAnyOrder("ROLE_USER", "ROLE_ADMIN");
    }

    @Test
    @DisplayName("SEC-02: Expired JWT token is deterministically rejected")
    void testExpiredToken() {
        Instant now = Instant.now();
        String expiredToken = jwtTokenService.generateCustomToken(
                "user-expired",
                List.of("ROLE_USER"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now.minus(Duration.ofHours(2)),
                now.minus(Duration.ofHours(1)),
                null
        );

        assertThatThrownBy(() -> jwtDecoder.decode(expiredToken))
                .isInstanceOf(org.springframework.security.oauth2.jwt.JwtException.class)
                .matches(e -> e.getMessage().toLowerCase().contains("expired"));
    }

    @Test
    @DisplayName("SEC-03: JWT with wrong issuer is rejected")
    void testWrongIssuer() {
        Instant now = Instant.now();
        String invalidToken = jwtTokenService.generateCustomToken(
                "user-attacker",
                List.of("ROLE_ADMIN"),
                "https://malicious.issuer.com",
                jwtProperties.getAudience(),
                now,
                now.plus(Duration.ofHours(1)),
                null
        );

        assertThatThrownBy(() -> jwtDecoder.decode(invalidToken))
                .isInstanceOf(JwtValidationException.class)
                .hasMessageContaining("iss");
    }

    @Test
    @DisplayName("SEC-04: JWT with wrong audience is rejected")
    void testWrongAudience() {
        Instant now = Instant.now();
        String invalidToken = jwtTokenService.generateCustomToken(
                "user-attacker",
                List.of("ROLE_ADMIN"),
                jwtProperties.getIssuer(),
                "unauthorized-api-audience",
                now,
                now.plus(Duration.ofHours(1)),
                null
        );

        assertThatThrownBy(() -> jwtDecoder.decode(invalidToken))
                .isInstanceOf(JwtValidationException.class)
                .hasMessageContaining("audience");
    }

    @Test
    @DisplayName("SEC-05: JWT signed with invalid signature / wrong RSA key is rejected")
    void testInvalidSignature() {
        RsaKeyProvider attackerProvider = new RsaKeyProvider();
        attackerProvider.rotateKey("attacker-key-1");
        Instant now = Instant.now();
        String forgedToken = attackerProvider.createSignedToken(
                "forged-admin",
                List.of("ROLE_ADMIN"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plus(Duration.ofHours(1)),
                "attacker-key-1"
        );

        assertThatThrownBy(() -> jwtDecoder.decode(forgedToken))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("SEC-06: JWT without 'ROLE_' prefix in claim gets mapped with 'ROLE_' prefix")
    void testRolePrefixMapping() {
        String token = jwtTokenService.generateToken("user-unprefixed", List.of("USER", "SERVICE"));
        Jwt jwt = jwtDecoder.decode(token);

        AbstractAuthenticationToken auth = securityConfig.jwtAuthenticationConverter().convert(jwt);
        assertThat(auth).isNotNull();
        List<String> authorities = auth.getAuthorities().stream().map(GrantedAuthority::getAuthority).toList();
        assertThat(authorities).containsExactlyInAnyOrder("ROLE_USER", "ROLE_SERVICE");
    }

    @Test
    @DisplayName("SEC-07: Unsigned token with algorithm 'none' is rejected immediately")
    void testAlgorithmNoneRejected() {
        String header = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString("{\"alg\":\"none\",\"typ\":\"JWT\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String payload = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString("{\"sub\":\"forged-admin\",\"roles\":[\"ADMIN\"],\"iss\":\"https://auth.dftp.bank\",\"aud\":[\"dftp-api\"]}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        String noneToken = header + "." + payload + ".";

        assertThatThrownBy(() -> jwtDecoder.decode(noneToken))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("SEC-08: Malformed non-list roles claim is gracefully handled with zero authorities (no privilege escalation)")
    void testMalformedNonListRolesClaim() {
        java.util.Map<String, Object> claims = new java.util.HashMap<>();
        claims.put("roles", "ADMIN"); // Single string instead of array
        Jwt jwt = new Jwt(
                "token-value",
                Instant.now(),
                Instant.now().plusSeconds(3600),
                java.util.Map.of("alg", "RS256"),
                claims
        );

        AbstractAuthenticationToken auth = securityConfig.jwtAuthenticationConverter().convert(jwt);
        assertThat(auth).isNotNull();
        assertThat(auth.getAuthorities()).isEmpty(); // No authorities assigned
    }

    @Test
    @DisplayName("SEC-09: Tampered JWT payload is rejected due to signature verification failure")
    void testTamperedPayloadRejected() {
        String validToken = jwtTokenService.generateToken("legit-user", List.of("ROLE_USER"));
        String[] parts = validToken.split("\\.");
        String tamperedPayload = java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                "{\"sub\":\"forged-admin\",\"roles\":[\"ROLE_ADMIN\"],\"iss\":\"https://auth.dftp.bank\",\"aud\":[\"dftp-api\"]}".getBytes(java.nio.charset.StandardCharsets.UTF_8)
        );
        String tamperedToken = parts[0] + "." + tamperedPayload + "." + parts[2];

        assertThatThrownBy(() -> jwtDecoder.decode(tamperedToken))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("SEC-10: Symmetric HS256 token is strictly rejected when RS256-only policy is active")
    void testHs256TokenRejectedByRs256Decoder() {
        Instant now = Instant.now();
        String hs256Token = jwtTokenService.generateLegacyHs256Token(
                "hs256-attacker",
                List.of("ROLE_ADMIN"),
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                now.plus(Duration.ofHours(1)),
                jwtProperties.getSecret()
        );

        assertThatThrownBy(() -> jwtDecoder.decode(hs256Token))
                .isInstanceOf(Exception.class);
    }
}
