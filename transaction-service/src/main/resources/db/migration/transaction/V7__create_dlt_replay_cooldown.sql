CREATE TABLE dlt_replay_cooldown (
    topic VARCHAR(255) PRIMARY KEY,
    last_replay_at TIMESTAMP WITH TIME ZONE NOT NULL,
    locked_by VARCHAR(255) NOT NULL
);
