-- Read-only role for the Grafana PostgreSQL data source "AtScale engine database".
--
-- Sample code, provided as is under the MIT License. Not an AtScale product. See DISCLAIMER.md.
--
-- Purpose: lets Grafana read the aggregate prediction reports (why AtScale did not propose an aggregate,
--   or rejected a candidate) and nothing else. SELECT on three engine tables, read-only transactions,
--   a statement timeout and a connection limit, so a dashboard cannot load the AtScale database.
-- Inputs: psql variable reader_password. Run it as the database owner on the AtScale database
--   (the database that has the schema "engine"). Running it again only resets the password and grants.
-- Run:
--   psql -U postgres -d atscale -v reader_password="$PASSWORD" -f grafana-reader.sql
\set ON_ERROR_STOP on

SELECT 'CREATE ROLE grafana_reader'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') \gexec

ALTER ROLE grafana_reader WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION
  CONNECTION LIMIT 5 PASSWORD :'reader_password';
ALTER ROLE grafana_reader SET default_transaction_read_only = on;
ALTER ROLE grafana_reader SET statement_timeout = '30s';

SELECT format('GRANT CONNECT ON DATABASE %I TO grafana_reader', current_database()) \gexec
GRANT USAGE ON SCHEMA engine TO grafana_reader;
GRANT SELECT ON engine.queries, engine.queries_planned, engine.aggregate_prediction_reports TO grafana_reader;
