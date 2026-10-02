-- The outbox row is an immutable snapshot of the event. Only delivery state
-- (status, attempts, scheduling, lease, publication and last error) may change.
CREATE OR REPLACE FUNCTION prevent_outbox_snapshot_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_id IS DISTINCT FROM OLD.event_id
        OR NEW.aggregate_type IS DISTINCT FROM OLD.aggregate_type
        OR NEW.aggregate_id IS DISTINCT FROM OLD.aggregate_id
        OR NEW.event_type IS DISTINCT FROM OLD.event_type
        OR NEW.correlation_id IS DISTINCT FROM OLD.correlation_id
        OR NEW.causation_id IS DISTINCT FROM OLD.causation_id
        OR NEW.occurred_at IS DISTINCT FROM OLD.occurred_at
        OR NEW.version IS DISTINCT FROM OLD.version
        OR NEW.payload IS DISTINCT FROM OLD.payload
        OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION 'outbox event snapshot is immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_outbox_snapshot_update
BEFORE UPDATE ON outbox_events
FOR EACH ROW
EXECUTE FUNCTION prevent_outbox_snapshot_mutation();
