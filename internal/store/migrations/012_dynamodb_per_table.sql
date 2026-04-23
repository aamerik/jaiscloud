-- Phase 1 placeholder: per-table DynamoDB schema support.
-- The actual per-table DDL (jc_dt_{suffix}, jc_dt_{suffix}_gsi, jc_dt_{suffix}_lsi)
-- is executed dynamically by PostgresDynamoDBItemStore.CreateTableSchema and
-- PostgresDynamoDBItemStore.DropTableSchema — not as a static migration — because
-- table names are user-defined and known only at runtime.
--
-- This migration creates no tables. It exists so the migration runner advances the
-- schema version and so the intent is recorded in version history.
SELECT 1;
