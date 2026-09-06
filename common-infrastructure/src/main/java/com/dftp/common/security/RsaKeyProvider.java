package com.dftp.common.security;

import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import lombok.SneakyThrows;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages asymmetric RSA-2048 keypairs for production OIDC / RS256 token signing and verification.
 * Supports Key ID (kid) labeling and cryptographic key rotation.
 */
@Component
public class RsaKeyProvider {

    private final Map<String, RSAKey> keyStore = new ConcurrentHashMap<>();
    private volatile String currentKeyId;

    private static final String DEFAULT_SHARED_RSA_KEY_JSON = """
            {"p":"7HUKzpX5AKFIRjH2f6CNvBsqYl9mN4B19W1WXiLoMNxsKIgrs0QjJcbD0D-GSSuHVwN8gUJSymdwT_o0CLWqYjlkMRT_W6j3iXti-LPFqjC7dK22J0YwzYOgJ1AjkmydphLJF59gtveHSIKcH3TK8FLA9S76POIeoaLM0uGdb5E","kty":"RSA","q":"y_scN6bJnNmLb-ukry0g5QedIC6Jpd8x5MjkV-zAoFuk_MR_wR62LQ6kCaU0hAz6UVLqSx02BHf7WKRQLGGiXet-F2AO1BqMsc-9WdLOTgwrxuEtoRN4X7ZLlfYO8j9fW3PezRBr4OKuRnTCncKDcz4kciWIR-s-m5CjeuDaqRs","d":"GisYcZ2WxJgxm7oHTJeMzZ6aOVfg7XYknNHLIE6dkHPZEwuoLzt_xH3vY0WveoKrJLAChoDtv9ToT05ij6CmMmLhCMhcknqh4cicOTW-5CjqW_KC82FaS3YeqZcEvthPlsMwN6o1I5pMBHp5qOHwuMcINRijsicLkivK_FE9q2i6EeTiwo-qBXwvJNUM9HA6QiZU2kR7ZrcqQgzph3lQqKNBNJX6edjCuAni5um6DhdgMOP5Qekqc6GP19904iVsYXNVRm0quFuafPHdFJsjLh1LsscZRP6VORbZf081mgly2oxkW9FKaqUylgIHNQ2Szko_MNpDRddwxVcZw7RjEQ","e":"AQAB","kid":"initial-key-1","qi":"RLQa7jK4FO64d2sAc5HEzpagWtl5tsjeRZTmXhH1tRh_P-M9jKO7xzoxQjXjdt2P4vWtrT-XJUTFPCKVwFT-MFfFuIsxb_p_fzL8aQy7-fvShqtUzoWA3vZKMn9ch3MCMajueyINAxlR3JQoDF5VZZ09zaLvkpqj4OgyK4Xg_l0","dp":"X6BV5oYwVVjkab76-VJs_43c7ju2kuuYyNCXBSsIy_nYo-uuAKmlrTldJ3MJU74O1dnLGFtCMCj0-uMs9_jAF0kug8sCGoeS1D7aH8pUPifHJ8C40dlJE1QQCNYYDLdwaiFewfbqnhQs2d44VWBeUiKldoKxIL4xtRypyP8FNIE","alg":"RS256","dq":"AoMCEvFb1DGdlPA64-lTWV6hoa7RBRjdfWq33RlOOetG-dHVnOQq7B0dCf1dyy55upyhw_EFJELx9mJ3lkRRgoSO1ezZ4eS9zMa5Fy0QW0etFlLZg_1AYzVUcvFHt9Xt76tal_3WxjskWDvB1b16buXZ_wnVRsVvzOyYBsBUEPc","n":"vGjAY53bhzSrDH2wARe0qQJmdGihIf1xWnHRHrCzSxbIBBIItyKw7cDNOkoSbUQ0kvP0bWM3C9g8h3ck6c1JiBSu3cxRW9Enw-Jp4eDQLBfdJnv35a8wsRTNIRtxzO2a3h5C0Z8Fvv0_SXz5N9RIsWZxFz2EBd7j3i1Ixjmup7jdQYkRx-Nh0ZFQXOtJ2DEPiKIpQ1jbXksj8gFFFVJzpg7c_OMAZbI0aWXaSBTz4uUHMZPfFigHcFEGPh2E8DdDtIxMwOP58uow_MERDUVJUBVPDN28vNKKEaKTq0RqsSogyTC-JgJYUXHZXETFxJ-0tuEWBqkxZyrGATD4m7t9Sw"}
            """;

    public RsaKeyProvider() {
        try {
            RSAKey rsaKey = RSAKey.parse(DEFAULT_SHARED_RSA_KEY_JSON.trim());
            addKey(rsaKey);
        } catch (Exception e) {
            System.err.println("RSA_KEY_PARSE_FAILED: " + e.getMessage());
            e.printStackTrace();
            rotateKey("initial-key-1");
        }
    }

    @SneakyThrows
    public synchronized String rotateKey(String newKeyId) {
        RSAKey rsaKey = new RSAKeyGenerator(2048)
                .keyID(newKeyId)
                .algorithm(JWSAlgorithm.RS256)
                .generate();
        keyStore.put(newKeyId, rsaKey);
        this.currentKeyId = newKeyId;
        return newKeyId;
    }

    public synchronized void revokeKey(String keyId) {
        keyStore.remove(keyId);
    }

    public RSAKey getCurrentPrivateKey() {
        return keyStore.get(currentKeyId);
    }

    public RSAKey getKey(String keyId) {
        return keyStore.get(keyId);
    }

    public String getCurrentKeyId() {
        return currentKeyId;
    }

    /**
     * Exposes the public keys as an RFC 7517 JWKS (JSON Web Key Set).
     */
    public JWKSet toPublicJwkSet() {
        List<com.nimbusds.jose.jwk.JWK> publicKeys = new ArrayList<>();
        for (RSAKey key : keyStore.values()) {
            publicKeys.add(key.toPublicJWK());
        }
        return new JWKSet(publicKeys);
    }

    public String toPublicJwksJson() {
        return toPublicJwkSet().toString();
    }

    public Map<String, Object> toPublicJwksMap() {
        return toPublicJwkSet().toJSONObject();
    }

    public synchronized void addKey(RSAKey rsaKey) {
        keyStore.put(rsaKey.getKeyID(), rsaKey);
        this.currentKeyId = rsaKey.getKeyID();
    }

    @SneakyThrows
    public String createSignedToken(
            String subject,
            List<String> roles,
            String issuer,
            String audience,
            Instant issueTime,
            Instant expirationTime,
            String keyId) {

        RSAKey signingKey = keyStore.get(keyId);
        if (signingKey == null) {
            throw new IllegalArgumentException("Signing key not found for keyId: " + keyId);
        }

        JWTClaimsSet.Builder claimsBuilder = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(audience)
                .issueTime(Date.from(issueTime))
                .expirationTime(Date.from(expirationTime))
                .notBeforeTime(Date.from(issueTime))
                .jwtID(UUID.randomUUID().toString())
                .claim("roles", roles);

        if (subject != null && !subject.trim().isEmpty()) {
            claimsBuilder.subject(subject);
        }

        JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID(signingKey.getKeyID())
                .type(com.nimbusds.jose.JOSEObjectType.JWT)
                .build();

        SignedJWT signedJWT = new SignedJWT(header, claimsBuilder.build());
        signedJWT.sign(new RSASSASigner(signingKey.toRSAPrivateKey()));
        return signedJWT.serialize();
    }
}
