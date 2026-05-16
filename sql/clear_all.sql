-- Clears source first (CDC propagates deletes to target), then force-clears target.
-- Ensures both sides are clean for a fresh test run.
--
-- Usage: psql "host=<endpoint> dbname=pocdb user=pocadmin password=<pwd>" -f sql/clear_all.sql

\echo '=== Step 1: Clearing source tables (will propagate to target via CDC) ==='
\i sql/clear_source.sql

\echo ''
\echo 'Waiting 20 seconds for CDC propagation...'
\! ping -n 21 127.0.0.1 > nul 2>&1 || sleep 20

\echo ''
\echo '=== Step 2: Force-clearing target tables ==='
\i sql/clear_target.sql

\echo ''
\echo 'Both schemas cleared. Run: python inject/inject.py seed'
