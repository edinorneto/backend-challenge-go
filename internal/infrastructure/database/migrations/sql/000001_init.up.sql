CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================================================
-- WALLET
-- =========================================================

CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_cents BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_wallet_currency
        CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT chk_wallet_balance_non_negative
        CHECK (balance_cents >= 0),

    CONSTRAINT chk_wallet_version_positive
        CHECK (version >= 1),

    CONSTRAINT uq_wallet_player_currency
        UNIQUE (player_id, currency)
);

-- =========================================================
-- WAGER TRANSACTIONS
-- =========================================================

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,

    source VARCHAR(20) NOT NULL,

    provider_id VARCHAR(100),
    external_transaction_id VARCHAR(255),
    idempotency_key VARCHAR(255),
    payload_hash VARCHAR(64),

    player_id UUID NOT NULL,
    wallet_id UUID NOT NULL,

    round_id VARCHAR(255),
    game_id VARCHAR(255),

    kind VARCHAR(20) NOT NULL,
    status VARCHAR(30) NOT NULL,

    amount_cents BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,

    reference_external_transaction_id VARCHAR(255),
    reference_transaction_id UUID,

    failure_code VARCHAR(100),

    result_balance_cents BIGINT,
    result_wallet_version BIGINT,

    reference_attempts INTEGER NOT NULL DEFAULT 0,
    reference_next_attempt_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,

    CONSTRAINT fk_wager_wallet
        FOREIGN KEY (wallet_id)
        REFERENCES wallets(id),

    CONSTRAINT chk_wager_source
        CHECK (source IN ('INTERNAL', 'EXTERNAL')),

    CONSTRAINT chk_wager_kind
        CHECK (
            kind IN (
                'OPENING',
                'BET',
                'WIN',
                'LOSS',
                'REFUND',
                'ROLLBACK'
            )
        ),

    CONSTRAINT chk_wager_status
        CHECK (
            status IN (
                'PENDING',
                'PENDING_REFERENCE',
                'PROCESSED',
                'REJECTED',
                'FAILED'
            )
        ),

    CONSTRAINT chk_wager_currency
        CHECK (currency ~ '^[A-Z]{3}$'),

    CONSTRAINT chk_wager_amount
        CHECK (
            (kind = 'OPENING' AND amount_cents >= 0)
            OR
            (kind = 'LOSS' AND amount_cents = 0)
            OR
            (
                kind IN ('BET', 'WIN', 'REFUND', 'ROLLBACK')
                AND amount_cents > 0
            )
        ),

    CONSTRAINT chk_wager_opening_internal
        CHECK (
            kind <> 'OPENING'
            OR (
                source = 'INTERNAL'
                AND provider_id IS NULL
                AND external_transaction_id IS NULL
                AND idempotency_key IS NULL
                AND payload_hash IS NULL
                AND round_id IS NULL
                AND game_id IS NULL
                AND reference_external_transaction_id IS NULL
                AND reference_transaction_id IS NULL
            )
        ),

    CONSTRAINT chk_wager_external_non_opening
        CHECK (
            kind = 'OPENING'
            OR (
                source = 'EXTERNAL'
                AND provider_id IS NOT NULL
                AND external_transaction_id IS NOT NULL
                AND idempotency_key IS NOT NULL
                AND payload_hash IS NOT NULL
            )
        ),

    CONSTRAINT chk_wager_reversal_reference
        CHECK (
            kind NOT IN ('REFUND', 'ROLLBACK')
            OR reference_external_transaction_id IS NOT NULL
        ),

    CONSTRAINT chk_reference_attempts_non_negative
        CHECK (reference_attempts >= 0)
);

CREATE UNIQUE INDEX uq_wager_provider_external_transaction
    ON wager_transactions(provider_id, external_transaction_id)
    WHERE provider_id IS NOT NULL
      AND external_transaction_id IS NOT NULL;

CREATE UNIQUE INDEX uq_wager_provider_idempotency_key
    ON wager_transactions(provider_id, idempotency_key)
    WHERE provider_id IS NOT NULL
      AND idempotency_key IS NOT NULL;

CREATE INDEX idx_wager_wallet
    ON wager_transactions(wallet_id);

CREATE INDEX idx_wager_status
    ON wager_transactions(status);

CREATE INDEX idx_wager_pending_reference
    ON wager_transactions(reference_next_attempt_at)
    WHERE status = 'PENDING_REFERENCE';

-- =========================================================
-- LEDGER
-- =========================================================

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,

    wallet_id UUID NOT NULL,
    transaction_id UUID NOT NULL,

    direction VARCHAR(10) NOT NULL,

    amount_cents BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,

    balance_before_cents BIGINT NOT NULL,
    balance_after_cents BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_ledger_wallet
        FOREIGN KEY (wallet_id)
        REFERENCES wallets(id),

    CONSTRAINT fk_ledger_transaction
        FOREIGN KEY (transaction_id)
        REFERENCES wager_transactions(id),

    CONSTRAINT chk_ledger_direction
        CHECK (direction IN ('DEBIT', 'CREDIT')),

    CONSTRAINT chk_ledger_amount_positive
        CHECK (amount_cents > 0),

    CONSTRAINT chk_ledger_balance_before_non_negative
        CHECK (balance_before_cents >= 0),

    CONSTRAINT chk_ledger_balance_after_non_negative
        CHECK (balance_after_cents >= 0),

    CONSTRAINT chk_ledger_balance_transition
        CHECK (
            (
                direction = 'DEBIT'
                AND balance_after_cents =
                    balance_before_cents - amount_cents
            )
            OR
            (
                direction = 'CREDIT'
                AND balance_after_cents =
                    balance_before_cents + amount_cents
            )
        ),

    CONSTRAINT uq_ledger_wallet_transaction
        UNIQUE (wallet_id, transaction_id)
);

CREATE INDEX idx_ledger_wallet_created_at
    ON wallet_ledger_entries(wallet_id, created_at DESC, id);

-- =========================================================
-- LEDGER IMMUTABILITY
-- =========================================================

CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'wallet ledger is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_ledger_update
BEFORE UPDATE ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TRIGGER trg_prevent_ledger_delete
BEFORE DELETE ON wallet_ledger_entries
FOR EACH ROW
EXECUTE FUNCTION prevent_ledger_mutation();

-- =========================================================
-- INBOX
-- =========================================================

CREATE TABLE inbox_messages (
    id UUID PRIMARY KEY,

    consumer_name VARCHAR(100) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    payload_hash VARCHAR(64) NOT NULL,

    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,

    CONSTRAINT uq_inbox_consumer_message
        UNIQUE (consumer_name, message_id)
);

CREATE INDEX idx_inbox_consumer
    ON inbox_messages(consumer_name);

-- =========================================================
-- OUTBOX
-- =========================================================

CREATE TABLE outbox_events (
    event_id UUID PRIMARY KEY,

    aggregate_type VARCHAR(100) NOT NULL,
    aggregate_id UUID NOT NULL,

    event_type VARCHAR(150) NOT NULL,

    correlation_id UUID NOT NULL,
    causation_id UUID,

    occurred_at TIMESTAMPTZ NOT NULL,
    version INTEGER NOT NULL,

    payload JSONB NOT NULL,

    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    locked_at TIMESTAMPTZ,
    locked_by VARCHAR(255),

    published_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_outbox_status
        CHECK (status IN ('PENDING', 'PUBLISHED')),

    CONSTRAINT chk_outbox_attempts_non_negative
        CHECK (attempts >= 0),

    CONSTRAINT chk_outbox_version_positive
        CHECK (version >= 1)
);

CREATE INDEX idx_outbox_pending
    ON outbox_events(status, next_attempt_at);

CREATE INDEX idx_outbox_locked
    ON outbox_events(locked_at)
    WHERE status = 'PENDING';