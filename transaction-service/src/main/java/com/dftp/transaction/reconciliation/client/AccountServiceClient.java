package com.dftp.transaction.reconciliation.client;

import com.dftp.common.model.SnapshotResult;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.reconciliation.client.dto.AccountSnapshotDto;
import com.dftp.transaction.reconciliation.client.dto.BulkAccountOperationsRequest;
import com.dftp.transaction.reconciliation.client.dto.BulkAccountOperationsResponse;
import com.dftp.transaction.reconciliation.client.dto.CandidateHoldDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.HttpServerErrorException;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
@Component
public class AccountServiceClient {

    private final RestTemplate restTemplate;
    private final JwtTokenService jwtTokenService;
    private final String accountServiceUrl;

    public static final int MAX_CHUNK_SIZE = 100;

    public AccountServiceClient(
            @Autowired(required = false) RestTemplate restTemplate,
            @Autowired(required = false) JwtTokenService jwtTokenService,
            @Value("${dftp.services.account-url:http://localhost:8081}") String accountServiceUrl) {
        this.restTemplate = restTemplate != null ? restTemplate : new RestTemplate();
        this.jwtTokenService = jwtTokenService;
        this.accountServiceUrl = accountServiceUrl;
    }

    /**
     * Account balance snapshot lookup with explicit failure semantics (P11-REM-01).
     */
    public SnapshotResult<AccountSnapshotDto> getAccountSnapshot(UUID accountId) {
        String url = accountServiceUrl + "/internal/reconciliation/accounts/" + accountId + "/snapshot";
        try {
            HttpEntity<?> entity = createAuthEntity(null);
            ResponseEntity<AccountSnapshotDto> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, AccountSnapshotDto.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return SnapshotResult.found(response.getBody());
            } else {
                return SnapshotResult.notFound("Account not found: " + accountId);
            }
        } catch (HttpClientErrorException.NotFound e) {
            log.debug("Account {} not found in account-service (404)", accountId);
            return SnapshotResult.notFound("Account not found (404)");
        } catch (HttpServerErrorException e) {
            log.warn("Account service 5xx error for account {}: {}", accountId, e.getStatusCode());
            return SnapshotResult.unavailable("Account service returned " + e.getStatusCode(), e.getStatusCode().value());
        } catch (ResourceAccessException e) {
            log.warn("Account service connection/timeout for account {}: {}", accountId, e.getMessage());
            return SnapshotResult.unavailable("Account service unavailable: " + e.getMessage(), 503);
        } catch (Exception e) {
            log.warn("Failed to fetch account snapshot for {}: {}", accountId, e.getMessage());
            return SnapshotResult.unavailable("Account lookup error: " + e.getMessage(), 500);
        }
    }

    /**
     * Single transaction operations lookup with explicit failure semantics (P11-REM-01).
     */
    public SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>> getOperationsByTransaction(String transactionId) {
        String url = accountServiceUrl + "/internal/reconciliation/operations/by-transaction/" + transactionId;
        try {
            HttpEntity<?> entity = createAuthEntity(null);
            ResponseEntity<List<AccountSnapshotDto.AccountOperationDto>> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, new ParameterizedTypeReference<>() {});
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return SnapshotResult.found(response.getBody());
            } else {
                return SnapshotResult.found(Collections.emptyList());
            }
        } catch (HttpClientErrorException.NotFound e) {
            return SnapshotResult.found(Collections.emptyList());
        } catch (HttpServerErrorException e) {
            log.warn("Account service 5xx error for tx {}: {}", transactionId, e.getStatusCode());
            return SnapshotResult.unavailable("Account service returned " + e.getStatusCode(), e.getStatusCode().value());
        } catch (ResourceAccessException e) {
            log.warn("Account service connection/timeout for tx {}: {}", transactionId, e.getMessage());
            return SnapshotResult.unavailable("Account service unavailable: " + e.getMessage(), 503);
        } catch (Exception e) {
            log.warn("Failed to fetch account operations for tx {}: {}", transactionId, e.getMessage());
            return SnapshotResult.unavailable("Account operations error: " + e.getMessage(), 500);
        }
    }

    /**
     * Candidate hold enumeration for reverse cross-service reconciliation (P11-REM-02).
     */
    public List<CandidateHoldDto> getOrphanHoldCandidates(long cutoffSeconds, int page, int size) {
        String url = accountServiceUrl + "/internal/reconciliation/accounts/orphan-hold-candidates?cutoffSeconds="
                + cutoffSeconds + "&page=" + page + "&size=" + size;
        try {
            HttpEntity<?> entity = createAuthEntity(null);
            ResponseEntity<List<CandidateHoldDto>> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, new ParameterizedTypeReference<>() {});
            return response.getBody() != null ? response.getBody() : Collections.emptyList();
        } catch (Exception e) {
            log.warn("Failed to fetch orphan hold candidates from account-service: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Bounded bulk operations lookup (P11-REM-03).
     * Partitions IDs into chunks of <= 100 to eliminate the 2N REST call bottleneck.
     */
    public Map<String, SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>>> getBulkOperations(List<String> transactionIds) {
        if (transactionIds == null || transactionIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>>> resultMap = new HashMap<>();

        for (int i = 0; i < transactionIds.size(); i += MAX_CHUNK_SIZE) {
            List<String> chunk = transactionIds.subList(i, Math.min(i + MAX_CHUNK_SIZE, transactionIds.size()));
            processBulkOperationsChunk(chunk, resultMap);
        }

        return resultMap;
    }

    private void processBulkOperationsChunk(
            List<String> chunk,
            Map<String, SnapshotResult<List<AccountSnapshotDto.AccountOperationDto>>> resultMap) {

        String url = accountServiceUrl + "/internal/reconciliation/accounts/bulk-operations";
        BulkAccountOperationsRequest request = new BulkAccountOperationsRequest(chunk);
        try {
            HttpEntity<BulkAccountOperationsRequest> entity = createAuthEntity(request);
            ResponseEntity<BulkAccountOperationsResponse> response = restTemplate.exchange(
                    url, HttpMethod.POST, entity, BulkAccountOperationsResponse.class);

            if (response.getBody() != null && response.getBody().getResults() != null) {
                Map<String, List<AccountSnapshotDto.AccountOperationDto>> results = response.getBody().getResults();
                for (String id : chunk) {
                    List<AccountSnapshotDto.AccountOperationDto> ops = results.getOrDefault(id, Collections.emptyList());
                    resultMap.put(id, SnapshotResult.found(ops));
                }
            } else {
                for (String id : chunk) {
                    resultMap.put(id, SnapshotResult.unavailable("No response body from bulk operations snapshot", 502));
                }
            }
        } catch (HttpClientErrorException.NotFound e) {
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.found(Collections.emptyList()));
            }
        } catch (HttpServerErrorException e) {
            log.warn("Bulk account operations 5xx error: {}", e.getStatusCode());
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.unavailable("Account service bulk error: " + e.getStatusCode(), e.getStatusCode().value()));
            }
        } catch (ResourceAccessException e) {
            log.warn("Bulk account operations connection/timeout: {}", e.getMessage());
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.unavailable("Account service unavailable: " + e.getMessage(), 503));
            }
        } catch (Exception e) {
            log.warn("Bulk account operations failed: {}", e.getMessage());
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.unavailable("Bulk lookup error: " + e.getMessage(), 500));
            }
        }
    }

    private <T> HttpEntity<T> createAuthEntity(T body) {
        HttpHeaders headers = new HttpHeaders();
        if (jwtTokenService != null) {
            String token = jwtTokenService.generateToken("transaction-service", List.of("ROLE_SERVICE"));
            headers.setBearerAuth(token);
        }
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new HttpEntity<>(body, headers);
    }
}
