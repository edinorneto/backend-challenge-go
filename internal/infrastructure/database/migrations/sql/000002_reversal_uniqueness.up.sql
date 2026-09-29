CREATE UNIQUE INDEX IF NOT EXISTS uq_wager_processed_reversal_reference_kind
    ON wager_transactions(provider_id, reference_external_transaction_id, kind)
    WHERE provider_id IS NOT NULL
      AND reference_external_transaction_id IS NOT NULL
      AND kind IN ('REFUND', 'ROLLBACK')
      AND status = 'PROCESSED';
