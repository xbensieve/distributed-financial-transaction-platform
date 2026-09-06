package com.dftp.common.security;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.HeadersConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2Error;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidatorResult;
import org.springframework.security.oauth2.jwt.*;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.web.SecurityFilterChain;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Configuration
@EnableWebSecurity
@EnableMethodSecurity
@ConditionalOnWebApplication
@RequiredArgsConstructor
public class SecurityConfig {

    private final JwtProperties jwtProperties;
    private final JsonAuthenticationEntryPoint authenticationEntryPoint;
    private final JsonAccessDeniedHandler accessDeniedHandler;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .csrf(AbstractHttpConfigurer::disable)
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .headers(headers -> headers
                        .frameOptions(HeadersConfigurer.FrameOptionsConfig::deny)
                        .contentTypeOptions(org.springframework.security.config.Customizer.withDefaults())
                )
                .authorizeHttpRequests(auth -> auth
                        .requestMatchers("/actuator/health/**", "/actuator/info", "/actuator/prometheus", "/actuator/metrics", "/actuator/metrics/**", "/.well-known/jwks.json").permitAll()
                        .requestMatchers("/admin/**").hasRole("ADMIN")
                        .requestMatchers(org.springframework.http.HttpMethod.POST, "/accounts/*/credit").hasRole("ADMIN")
                        .requestMatchers("/transactions/**").hasAnyRole("USER", "SERVICE", "ADMIN")
                        .requestMatchers("/accounts/**").hasAnyRole("USER", "SERVICE", "ADMIN")
                        .requestMatchers("/ledger/**").hasAnyRole("SERVICE", "ADMIN")
                        .anyRequest().authenticated()
                )
                .exceptionHandling(exceptions -> exceptions
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler)
                )
                .oauth2ResourceServer(oauth2 -> oauth2
                        .jwt(jwt -> jwt
                                .jwtAuthenticationConverter(jwtAuthenticationConverter())
                        )
                        .authenticationEntryPoint(authenticationEntryPoint)
                        .accessDeniedHandler(accessDeniedHandler)
                );

        return http.build();
    }

    @Bean
    public JwtDecoder jwtDecoder(RsaKeyProvider rsaKeyProvider) {
        NimbusJwtDecoder decoder;
        if (jwtProperties.getJwkSetUri() != null && !jwtProperties.getJwkSetUri().isBlank()) {
            decoder = NimbusJwtDecoder.withJwkSetUri(jwtProperties.getJwkSetUri())
                    .jwsAlgorithm(org.springframework.security.oauth2.jose.jws.SignatureAlgorithm.RS256)
                    .build();
        } else {
            com.nimbusds.jose.jwk.source.JWKSource<com.nimbusds.jose.proc.SecurityContext> jwkSource =
                    (jwkSelector, context) -> jwkSelector.select(rsaKeyProvider.toPublicJwkSet());
            com.nimbusds.jwt.proc.ConfigurableJWTProcessor<com.nimbusds.jose.proc.SecurityContext> jwtProcessor =
                    new com.nimbusds.jwt.proc.DefaultJWTProcessor<>();
            com.nimbusds.jose.proc.JWSKeySelector<com.nimbusds.jose.proc.SecurityContext> keySelector =
                    new com.nimbusds.jose.proc.JWSVerificationKeySelector<>(com.nimbusds.jose.JWSAlgorithm.RS256, jwkSource);
            jwtProcessor.setJWSKeySelector(keySelector);
            decoder = new NimbusJwtDecoder(jwtProcessor);
        }

        OAuth2TokenValidator<Jwt> issuerValidator = new JwtIssuerValidator(jwtProperties.getIssuer());
        OAuth2TokenValidator<Jwt> timestampValidator = new JwtTimestampValidator(
                Duration.ofSeconds(jwtProperties.getClockSkewSeconds())
        );

        OAuth2TokenValidator<Jwt> audienceValidator = token -> {
            List<String> aud = token.getAudience();
            if (aud != null && aud.contains(jwtProperties.getAudience())) {
                return OAuth2TokenValidatorResult.success();
            }
            return OAuth2TokenValidatorResult.failure(
                    new OAuth2Error("invalid_audience", "The token audience does not match: " + jwtProperties.getAudience(), null)
            );
        };

        OAuth2TokenValidator<Jwt> subjectValidator = token -> {
            if (token.getSubject() != null && !token.getSubject().trim().isEmpty()) {
                return OAuth2TokenValidatorResult.success();
            }
            return OAuth2TokenValidatorResult.failure(
                    new OAuth2Error("invalid_subject", "The token subject must not be null or empty", null)
            );
        };

        OAuth2TokenValidator<Jwt> combinedValidator = new DelegatingOAuth2TokenValidator<>(
                timestampValidator,
                issuerValidator,
                audienceValidator,
                subjectValidator
        );

        decoder.setJwtValidator(combinedValidator);
        return decoder;
    }

    public Converter<Jwt, AbstractAuthenticationToken> jwtAuthenticationConverter() {
        return jwt -> {
            Collection<GrantedAuthority> authorities = extractAuthorities(jwt);
            return new JwtAuthenticationToken(jwt, authorities, jwt.getSubject());
        };
    }

    @SuppressWarnings("unchecked")
    private Collection<GrantedAuthority> extractAuthorities(Jwt jwt) {
        Object rolesClaim = jwt.getClaims().get("roles");
        if (rolesClaim instanceof List<?> roleList) {
            return roleList.stream()
                    .map(Object::toString)
                    .map(role -> role.startsWith("ROLE_") ? role : "ROLE_" + role)
                    .map(SimpleGrantedAuthority::new)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}
