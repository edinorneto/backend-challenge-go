-- At most one OPENING (initial credit) per wallet, enforced by the schema and
-- not only by the code that opens wallets.
CREATE UNIQUE INDEX uq_wager_opening_per_wallet
    ON wager_transactions(wallet_id)
    WHERE kind = 'OPENING';
