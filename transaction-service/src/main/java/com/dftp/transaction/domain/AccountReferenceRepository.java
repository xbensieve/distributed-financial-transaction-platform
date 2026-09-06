package com.dftp.transaction.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.UUID;

@Repository
public interface AccountReferenceRepository extends JpaRepository<AccountReference, UUID> {
}
