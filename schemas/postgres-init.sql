-- PostgreSQL Schema for Fraud Detection Pipeline
-- Stores scored transactions for audit trail and analytics

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- CORE TABLES
-- ============================================================================

-- Scored transactions (main audit trail)
CREATE TABLE scored_transactions (
    id                      BIGSERIAL PRIMARY KEY,
    transaction_id          VARCHAR(64) UNIQUE NOT NULL,
    user_id                 VARCHAR(64) NOT NULL,
    amount                  DECIMAL(18, 2) NOT NULL,
    currency                VARCHAR(3) NOT NULL,
    payment_rail            VARCHAR(32) NOT NULL,
    country_code            VARCHAR(2) NOT NULL,
    
    -- Fraud scoring results
    fraud_score             DECIMAL(5, 4) NOT NULL,  -- 0.0000 to 1.0000
    risk_level              VARCHAR(16) NOT NULL,
    decision                VARCHAR(16) NOT NULL,
    rules_triggered         TEXT[],  -- Array of rule names
    
    -- Features (denormalized for analytics)
    txn_count_1h            INTEGER,
    txn_amount_avg_7d       DECIMAL(18, 2),
    unique_merchants_24h    INTEGER,
    time_since_last_txn_sec BIGINT,
    device_age_days         INTEGER,
    cross_border_ratio_30d  DECIMAL(5, 4),
    
    -- Metadata
    model_version           VARCHAR(32),
    processing_latency_ms   INTEGER,
    transaction_time        TIMESTAMP WITH TIME ZONE NOT NULL,
    scored_at               TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Indexes for common queries
    CONSTRAINT valid_fraud_score CHECK (fraud_score >= 0 AND fraud_score <= 1),
    CONSTRAINT valid_risk_level CHECK (risk_level IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    CONSTRAINT valid_decision CHECK (decision IN ('APPROVE', 'REVIEW', 'DECLINE', 'CHALLENGE'))
);

-- Indexes for analytics queries
CREATE INDEX idx_scored_txn_user ON scored_transactions(user_id, transaction_time DESC);
CREATE INDEX idx_scored_txn_time ON scored_transactions(transaction_time DESC);
CREATE INDEX idx_scored_txn_risk ON scored_transactions(risk_level, transaction_time DESC);
CREATE INDEX idx_scored_txn_payment_rail ON scored_transactions(payment_rail, transaction_time DESC);
CREATE INDEX idx_scored_txn_country ON scored_transactions(country_code, transaction_time DESC);

-- User feature history (for offline analysis)
CREATE TABLE user_features_snapshot (
    id                      BIGSERIAL PRIMARY KEY,
    user_id                 VARCHAR(64) NOT NULL,
    snapshot_time           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    txn_count_1h            INTEGER,
    txn_count_24h           INTEGER,
    txn_count_7d            INTEGER,
    txn_amount_avg_7d       DECIMAL(18, 2),
    txn_amount_max_30d      DECIMAL(18, 2),
    unique_merchants_24h    INTEGER,
    unique_devices_30d      INTEGER,
    cross_border_ratio_30d  DECIMAL(5, 4),
    
    -- For training data extraction
    CONSTRAINT unique_user_snapshot UNIQUE (user_id, snapshot_time)
);

CREATE INDEX idx_user_features_user ON user_features_snapshot(user_id, snapshot_time DESC);

-- Rule trigger audit log
CREATE TABLE rule_triggers (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      VARCHAR(64) NOT NULL REFERENCES scored_transactions(transaction_id),
    rule_name           VARCHAR(64) NOT NULL,
    rule_version        VARCHAR(16),
    trigger_details     JSONB,  -- Rule-specific context
    triggered_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_rule_triggers_txn ON rule_triggers(transaction_id);
CREATE INDEX idx_rule_triggers_rule ON rule_triggers(rule_name, triggered_at DESC);

-- ============================================================================
-- VIEWS FOR DASHBOARDS
-- ============================================================================

-- Hourly fraud summary (for Grafana)
CREATE VIEW hourly_fraud_summary AS
SELECT 
    DATE_TRUNC('hour', transaction_time) AS hour,
    payment_rail,
    country_code,
    COUNT(*) AS total_transactions,
    COUNT(*) FILTER (WHERE risk_level IN ('HIGH', 'CRITICAL')) AS high_risk_count,
    COUNT(*) FILTER (WHERE decision = 'DECLINE') AS declined_count,
    AVG(fraud_score) AS avg_fraud_score,
    AVG(processing_latency_ms) AS avg_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY processing_latency_ms) AS p99_latency_ms
FROM scored_transactions
WHERE transaction_time > NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', transaction_time), payment_rail, country_code;

-- Rule effectiveness summary
CREATE VIEW rule_effectiveness AS
SELECT 
    rt.rule_name,
    COUNT(*) AS trigger_count,
    COUNT(*) FILTER (WHERE st.decision = 'DECLINE') AS decline_after_trigger,
    AVG(st.fraud_score) AS avg_score_when_triggered
FROM rule_triggers rt
JOIN scored_transactions st ON rt.transaction_id = st.transaction_id
WHERE rt.triggered_at > NOW() - INTERVAL '30 days'
GROUP BY rt.rule_name;

-- ============================================================================
-- SAMPLE DATA FOR TESTING
-- ============================================================================

-- Insert sample data (will be populated by pipeline)
INSERT INTO scored_transactions (
    transaction_id, user_id, amount, currency, payment_rail, country_code,
    fraud_score, risk_level, decision, rules_triggered,
    txn_count_1h, txn_amount_avg_7d, unique_merchants_24h,
    time_since_last_txn_sec, device_age_days, cross_border_ratio_30d,
    model_version, processing_latency_ms, transaction_time
) VALUES (
    'txn_sample_001', 'user_test_001', 150.00, 'THB', 'PROMPTPAY', 'TH',
    0.12, 'LOW', 'APPROVE', ARRAY[]::TEXT[],
    3, 125.50, 2, 3600, 45, 0.05,
    'v1.0.0', 45, NOW()
);
