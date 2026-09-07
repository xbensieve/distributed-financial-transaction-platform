package com.dftp.transaction.reconciliation.client;

import com.dftp.common.model.SnapshotResult;
import com.dftp.common.security.JwtTokenService;
import com.dftp.transaction.reconciliation.client.dto.BulkLedgerSnapshotRequest;
import com.dftp.transaction.reconciliation.client.dto.BulkLedgerSnapshotResponse;
import com.dftp.transaction.reconciliation.client.dto.CandidateLedgerTransactionDto;
import com.dftp.transaction.reconciliation.client.dto.LedgerSnapshotDto;
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
public class LedgerServiceClient {

    private final RestTemplate restTemplate;
    private final JwtTokenService jwtTokenService;
    private final String ledgerServiceUrl;

    public static final int MAX_CHUNK_SIZE = 100;

    public LedgerServiceClient(
            @Autowired(required = false) RestTemplate restTemplate,
            @Autowired(required = false) JwtTokenService jwtTokenService,
            @Value("${dftp.services.ledger-url:http://localhost:8083}") String ledgerServiceUrl) {
        this.restTemplate = restTemplate != null ? restTemplate : new RestTemplate();
        this.jwtTokenService = jwtTokenService;
        this.ledgerServiceUrl = ledgerServiceUrl;
    }

    /**
     * Single transaction ledger snapshot lookup with explicit failure semantics (P11-REM-01).
     * Distinguishes 404 (NOT_FOUND) from 503/timeout (DEPENDENCY_UNAVAILABLE).
     */
    public SnapshotResult<LedgerSnapshotDto> getLedgerSnapshot(String businessTransactionId) {
        String url = ledgerServiceUrl + "/internal/reconciliation/ledger/by-transaction/" + businessTransactionId;
        try {
            HttpEntity<?> entity = createAuthEntity(null);
            ResponseEntity<LedgerSnapshotDto> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, LedgerSnapshotDto.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                return SnapshotResult.found(response.getBody());
            } else {
                return SnapshotResult.notFound("Ledger transaction not found: " + businessTransactionId);
            }
        } catch (HttpClientErrorException.NotFound e) {
            log.debug("Ledger transaction {} not found in ledger-service (404)", businessTransactionId);
            return SnapshotResult.notFound("Ledger transaction not found (404)");
        } catch (HttpServerErrorException e) {
            log.warn("Ledger service 5xx error when fetching tx {}: {} ({})", businessTransactionId, e.getStatusCode(), e.getMessage());
            return SnapshotResult.unavailable("Ledger service returned " + e.getStatusCode(), e.getStatusCode().value());
        } catch (ResourceAccessException e) {
            log.warn("Ledger service connection/timeout when fetching tx {}: {}", businessTransactionId, e.getMessage());
            return SnapshotResult.unavailable("Ledger service unavailable: " + e.getMessage(), 503);
        } catch (Exception e) {
            log.warn("Failed to fetch ledger snapshot for tx {}: {}", businessTransactionId, e.getMessage());
            return SnapshotResult.unavailable("Ledger lookup error: " + e.getMessage(), 500);
        }
    }

    /**
     * Candidate enumeration for reverse cross-service reconciliation (P11-REM-02).
     * Fetches candidate ledger transactions older than cutoff.
     */
    public List<CandidateLedgerTransactionDto> getUnmatchedCandidates(long cutoffSeconds, int page, int size) {
        String url = ledgerServiceUrl + "/internal/reconciliation/ledger/unmatched-candidates?cutoffSeconds="
                + cutoffSeconds + "&page=" + page + "&size=" + size;
        try {
            HttpEntity<?> entity = createAuthEntity(null);
            ResponseEntity<List<CandidateLedgerTransactionDto>> response = restTemplate.exchange(
                    url, HttpMethod.GET, entity, new ParameterizedTypeReference<>() {});
            return response.getBody() != null ? response.getBody() : Collections.emptyList();
        } catch (Exception e) {
            log.warn("Failed to fetch unmatched candidates from ledger-service: {}", e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * Bounded bulk snapshot lookup (P11-REM-03).
     * Partitions IDs into chunks of <= 100 to eliminate the 2N REST call bottleneck.
     */
    public Map<String, SnapshotResult<LedgerSnapshotDto>> getBulkLedgerSnapshots(List<String> businessTransactionIds) {
        if (businessTransactionIds == null || businessTransactionIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Map<String, SnapshotResult<LedgerSnapshotDto>> resultMap = new HashMap<>();

        // Partition into bounded chunks of at most MAX_CHUNK_SIZE (100)
        for (int i = 0; i < businessTransactionIds.size(); i += MAX_CHUNK_SIZE) {
            List<String> chunk = businessTransactionIds.subList(i, Math.min(i + MAX_CHUNK_SIZE, businessTransactionIds.size()));
            processBulkChunk(chunk, resultMap);
        }

        return resultMap;
    }

    private void processBulkChunk(List<String> chunk, Map<String, SnapshotResult<LedgerSnapshotDto>> resultMap) {
        String url = ledgerServiceUrl + "/internal/reconciliation/ledger/bulk-snapshot";
        BulkLedgerSnapshotRequest request = new BulkLedgerSnapshotRequest(chunk);
        try {
            HttpEntity<BulkLedgerSnapshotRequest> entity = createAuthEntity(request);
            ResponseEntity<BulkLedgerSnapshotResponse> response = restTemplate.exchange(
                    url, HttpMethod.POST, entity, BulkLedgerSnapshotResponse.class);

            if (response.getBody() != null && response.getBody().getResults() != null) {
                for (BulkLedgerSnapshotResponse.Item item : response.getBody().getResults()) {
                    if ("FOUND".equalsIgnoreCase(item.getStatus()) && item.getSnapshot() != null) {
                        resultMap.put(item.getBusinessTransactionId(), SnapshotResult.found(item.getSnapshot()));
                    } else {
                        resultMap.put(item.getBusinessTransactionId(),
                                SnapshotResult.notFound("Ledger record not found"));
                    }
                }
            } else {
                for (String id : chunk) {
                    resultMap.put(id, SnapshotResult.unavailable("No response body from bulk snapshot", 502));
                }
            }
        } catch (HttpClientErrorException.NotFound e) {
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.notFound("Bulk endpoint returned 404"));
            }
        } catch (HttpServerErrorException e) {
            log.warn("Bulk ledger snapshot 5xx error: {}", e.getStatusCode());
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.unavailable("Ledger service bulk error: " + e.getStatusCode(), e.getStatusCode().value()));
            }
        } catch (ResourceAccessException e) {
            log.warn("Bulk ledger snapshot connection/timeout: {}", e.getMessage());
            for (String id : chunk) {
                resultMap.put(id, SnapshotResult.unavailable("Ledger service unavailable: " + e.getMessage(), 503));
            }
        } catch (Exception e) {
            log.warn("Bulk ledger snapshot failed: {}", e.getMessage());
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
