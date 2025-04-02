-- Ensure WAL settings are applied
ALTER SYSTEM SET wal_level = 'logical';
ALTER SYSTEM SET max_wal_senders = 10;
ALTER SYSTEM SET max_replication_slots = 10;
SELECT pg_reload_conf();

-- Wait for settings to apply before creating the slot
DO $$ 
BEGIN 
  PERFORM pg_sleep(5);  -- Ensure WAL settings are reloaded
END $$;

-- Create the logical replication slot
SELECT * FROM pg_create_logical_replication_slot('olake_slot', 'wal2json');

