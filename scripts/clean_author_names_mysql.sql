USE research_mysql2;

-- Cleans leading/trailing list/quote artifacts from author names in a small
-- batch to avoid long locks.
--
-- Run this after taking a backup:
--   mysql -u root -p research_mysql2 < scripts/clean_author_names_mysql.sql
--
-- Run it repeatedly until changed_rows returns 0.
-- Uses SKIP LOCKED so an open app/Workbench transaction does not block the
-- whole cleanup batch. Requires MySQL 8.0+.

SET SESSION innodb_lock_wait_timeout = 5;

START TRANSACTION;

CREATE TEMPORARY TABLE author_cleanup_batch (
  author_id INT PRIMARY KEY,
  cleaned_author_name VARCHAR(255) NOT NULL
) ENGINE = MEMORY;

INSERT INTO author_cleanup_batch (author_id, cleaned_author_name)
SELECT author_id,
       TRIM(
         REGEXP_REPLACE(
           REGEXP_REPLACE(author_name, '^[[:space:]\\[\\]''"]+|[[:space:]\\[\\]''"]+$', ''),
           '[[:space:]]+',
           ' '
         )
       ) AS cleaned_author_name
FROM authors
WHERE author_name REGEXP '^[[:space:]\\[\\]''"]+|[[:space:]\\[\\]''"]+$|[[:space:]]{2,}'
ORDER BY author_id
LIMIT 25
FOR UPDATE SKIP LOCKED;

UPDATE authors
JOIN author_cleanup_batch dirty ON dirty.author_id = authors.author_id
SET authors.author_name = dirty.cleaned_author_name;

SELECT ROW_COUNT() AS changed_rows;

DROP TEMPORARY TABLE author_cleanup_batch;

COMMIT;

SELECT author_id, author_name
FROM authors
WHERE author_name REGEXP '^[[:space:]\\[\\]''"]+|[[:space:]\\[\\]''"]+$'
ORDER BY author_name
LIMIT 25;
