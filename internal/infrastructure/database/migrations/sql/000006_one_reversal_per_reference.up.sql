-- A processed transaction is reversed at most once, whatever the reversal
-- kind: a BET by one REFUND or one ROLLBACK, a WIN or a REFUND by one ROLLBACK.
-- The repository already checks this under the lock of the referenced row; the
-- index makes it a schema rule too.
CREATE UNIQUE INDEX uq_wager_processed_reversal_reference
    ON wager_transactions(reference_transaction_id)
    WHERE status = 'PROCESSED'
      AND kind IN ('REFUND', 'ROLLBACK');
