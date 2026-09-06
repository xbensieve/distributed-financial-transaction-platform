package com.dftp.account.api;

import com.dftp.account.api.dto.AccountResponse;
import com.dftp.account.api.dto.CreateAccountRequest;
import com.dftp.account.application.AccountApplicationService;
import com.dftp.account.domain.Account;
import com.dftp.account.domain.AccountRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountApplicationService accountApplicationService;
    private final AccountRepository accountRepository;

    @PostMapping
    public ResponseEntity<AccountResponse> createAccount(
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId,
            @Valid @RequestBody CreateAccountRequest request) {
        
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }

        log.info("Received request to create account with transactionId: {}, correlationId: {}", 
                request.getTransactionId(), correlationId);

        Account account = accountApplicationService.createAccount(request, correlationId);
        
        AccountResponse response = AccountResponse.builder()
                .accountId(account.getId())
                .transactionId(account.getTransactionId())
                .status(account.getStatus())
                .createdAt(account.getCreatedAt())
                .settledBalance(account.getSettledBalance())
                .heldFunds(account.getHeldFunds())
                .ownerId(account.getOwnerId())
                .build();

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @GetMapping("/{accountId}")
    public ResponseEntity<AccountResponse> getAccount(@PathVariable UUID accountId) {
        Account account = accountRepository.findById(accountId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found"));

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        boolean isPrivileged = auth != null && auth.getAuthorities().stream()
                .anyMatch(a -> a.getAuthority().equals("ROLE_ADMIN") || a.getAuthority().equals("ROLE_SERVICE"));
        boolean isUnassigned = "system-unassigned".equals(account.getOwnerId());
        boolean isOwner = auth != null && !isUnassigned && account.getOwnerId().equals(auth.getName());

        if (!isPrivileged && !isOwner) {
            log.warn("SECURITY ALERT: Principal '{}' attempted unauthorized access to account '{}' (owner: '{}')",
                    auth != null ? auth.getName() : "ANONYMOUS", accountId, account.getOwnerId());
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Account not found");
        }

        return ResponseEntity.ok(AccountResponse.builder()
                .accountId(account.getId())
                .transactionId(account.getTransactionId())
                .status(account.getStatus())
                .createdAt(account.getCreatedAt())
                .settledBalance(account.getSettledBalance())
                .heldFunds(account.getHeldFunds())
                .ownerId(account.getOwnerId())
                .build());
    }

    @PreAuthorize("hasRole('ADMIN')")
    @PostMapping("/{accountId}/credit")
    public ResponseEntity<Void> creditAccount(
            @PathVariable UUID accountId,
            @RequestParam BigDecimal amount) {
        accountApplicationService.creditAccount(accountId, amount);
        return ResponseEntity.ok().build();
    }
}
