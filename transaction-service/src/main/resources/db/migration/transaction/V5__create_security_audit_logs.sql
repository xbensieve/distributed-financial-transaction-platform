CREATE TABLE security_audit_logs (
    id UUID PRIMARY KEY,
    principal VARCHAR(255) NOT NULL,
    roles VARCHAR(255),
    action VARCHAR(100) NOT NULL,
    target_resource VARCHAR(255) NOT NULL,
    details JSONB,
    status VARCHAR(50) NOT NULL,
    client_ip VARCHAR(50),
    correlation_id VARCHAR(255),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_security_audit_logs_action ON security_audit_logs(action);
CREATE INDEX idx_security_audit_logs_created_at ON security_audit_logs(created_at);
