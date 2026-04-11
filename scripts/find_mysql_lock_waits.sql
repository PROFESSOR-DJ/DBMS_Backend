-- Shows MySQL transactions and lock waits that may block author cleanup.
-- Run in MySQL Workbench or mysql CLI:
--   mysql -u root -p research_mysql2 < scripts/find_mysql_lock_waits.sql

SELECT trx_id,
       trx_state,
       trx_started,
       trx_mysql_thread_id,
       trx_query
FROM information_schema.innodb_trx
ORDER BY trx_started;

SELECT waiting_pid,
       blocking_pid,
       waiting_query,
       blocking_query
FROM sys.innodb_lock_waits;

-- If a blocking_pid is clearly an old stuck session, kill it manually:
--   KILL <blocking_pid>;
