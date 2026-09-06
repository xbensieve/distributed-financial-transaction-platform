package com.dftp.template;

import java.math.BigDecimal;

public class TestBusinessEvent {
    private String accountId;
    private BigDecimal amount;
    private String description;

    public TestBusinessEvent() {}

    public TestBusinessEvent(String accountId, BigDecimal amount, String description) {
        this.accountId = accountId;
        this.amount = amount;
        this.description = description;
    }

    public String getAccountId() { return accountId; }
    public void setAccountId(String accountId) { this.accountId = accountId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
}
