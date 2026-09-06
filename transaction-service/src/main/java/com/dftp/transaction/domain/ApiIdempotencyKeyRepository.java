package com.dftp.transaction.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface ApiIdempotencyKeyRepository extends JpaRepository<ApiIdempotencyKey, String> {
}
