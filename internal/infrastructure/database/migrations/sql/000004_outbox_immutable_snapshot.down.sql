DROP TRIGGER IF EXISTS trg_prevent_outbox_snapshot_update ON outbox_events;
DROP FUNCTION IF EXISTS prevent_outbox_snapshot_mutation();
