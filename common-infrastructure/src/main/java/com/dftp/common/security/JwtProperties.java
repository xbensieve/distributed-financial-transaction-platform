package com.dftp.common.security;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "dftp.security.jwt")
public class JwtProperties {

    /**
     * Shared HMAC-SHA256 secret key (at least 256 bits / 32 bytes).
     * In production, this must be injected via DFTP_SECURITY_JWT_SECRET environment variable.
     */
    private String secret = "dftp-dev-secret-key-must-be-at-least-32-bytes-long-for-hmac256";

    /**
     * Expected token issuer claim (iss).
     */
    private String issuer = "https://auth.dftp.bank";

    /**
     * Expected token audience claim (aud).
     */
    private String audience = "dftp-api";

    /**
     * Default token time-to-live in seconds (1 hour).
     */
    private long expirationSeconds = 3600;

    /**
     * Allowed clock skew for timestamp validation in seconds.
     */
    private long clockSkewSeconds = 60;

    /**
     * Optional external OIDC / JWKS URI (e.g. Keycloak / Okta in production).
     * If provided, NimbusJwtDecoder will fetch public keys from this URI.
     */
    private String jwkSetUri;

    /**
     * Enforce RS256-only policy (reject HS256 tokens). Default: true.
     */
    private boolean rs256Only = true;
}
