package com.dftp.common.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * Service for generating signed JWTs for internal service communication and testing.
 * Standardized on asymmetric RS256 signing via RsaKeyProvider.
 */
@Service
public class JwtTokenService {

    private final JwtProperties jwtProperties;
    private final RsaKeyProvider rsaKeyProvider;

    @Autowired
    public JwtTokenService(JwtProperties jwtProperties, RsaKeyProvider rsaKeyProvider) {
        this.jwtProperties = jwtProperties;
        this.rsaKeyProvider = rsaKeyProvider;
    }

    public JwtTokenService(JwtProperties jwtProperties) {
        this(jwtProperties, new RsaKeyProvider());
    }

    /**
     * Generates a signed RS256 JWT with default issuer, audience, and configured TTL.
     */
    public String generateToken(String subject, List<String> roles) {
        return generateToken(subject, roles, Duration.ofSeconds(jwtProperties.getExpirationSeconds()));
    }

    /**
     * Generates a signed RS256 JWT with default issuer, audience, and custom TTL.
     */
    public String generateToken(String subject, List<String> roles, Duration ttl) {
        Instant now = Instant.now();
        Instant expiry = now.plus(ttl);
        return rsaKeyProvider.createSignedToken(
                subject,
                roles,
                jwtProperties.getIssuer(),
                jwtProperties.getAudience(),
                now,
                expiry,
                rsaKeyProvider.getCurrentKeyId()
        );
    }

    /**
     * Generates a custom RS256 token for boundary/adversarial testing.
     */
    public String generateCustomToken(
            String subject,
            List<String> roles,
            String issuer,
            String audience,
            Instant issuedAt,
            Instant expiresAt,
            String keyId) {

        return rsaKeyProvider.createSignedToken(
                subject,
                roles,
                issuer,
                audience,
                issuedAt,
                expiresAt,
                keyId != null ? keyId : rsaKeyProvider.getCurrentKeyId()
        );
    }

    /**
     * Generates a legacy symmetric HS256 token to verify that the RS256 decoder rejects it.
     */
    public String generateLegacyHs256Token(
            String subject,
            List<String> roles,
            String issuer,
            String audience,
            Instant issuedAt,
            Instant expiresAt,
            String signingSecret) {

        try {
            JWTClaimsSet claimsSet = new JWTClaimsSet.Builder()
                    .jwtID(UUID.randomUUID().toString())
                    .subject(subject)
                    .issuer(issuer)
                    .audience(Collections.singletonList(audience))
                    .issueTime(Date.from(issuedAt))
                    .expirationTime(Date.from(expiresAt))
                    .claim("roles", roles)
                    .build();

            SignedJWT signedJWT = new SignedJWT(
                    new JWSHeader(JWSAlgorithm.HS256),
                    claimsSet
            );

            MACSigner signer = new MACSigner(signingSecret.getBytes(StandardCharsets.UTF_8));
            signedJWT.sign(signer);

            return signedJWT.serialize();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to generate signed HS256 JWT", e);
        }
    }
}
