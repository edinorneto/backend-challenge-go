-- TRUNCATE does not fire row-level triggers, so the UPDATE and DELETE guards of
-- 000001 do not cover it. A statement-level trigger closes that path.
CREATE TRIGGER trg_prevent_ledger_truncate
BEFORE TRUNCATE ON wallet_ledger_entries
FOR EACH STATEMENT
EXECUTE FUNCTION prevent_ledger_mutation();
