package com.dftp.common.security;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Exposes RFC 7517 public JWKS (JSON Web Key Set) for asymmetric RS256 token verification.
 * Does not expose any private key material.
 */
@RestController
@RequestMapping("/.well-known")
@ConditionalOnWebApplication
@RequiredArgsConstructor
public class JwksController {

    private final RsaKeyProvider rsaKeyProvider;

    @GetMapping(value = "/jwks.json", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> getPublicJwks() {
        return ResponseEntity.ok(rsaKeyProvider.toPublicJwksMap());
    }
}
