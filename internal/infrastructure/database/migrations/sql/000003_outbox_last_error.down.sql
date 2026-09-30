ALTER TABLE outbox_events
    DROP COLUMN IF EXISTS last_error;
