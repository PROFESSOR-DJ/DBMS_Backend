USE research_mysql2;

-- Optional follow-up after clean_author_names_mysql.sql returns changed_rows = 0.
-- Merges a small batch of duplicate authors that now have the same author_name.
--
-- Run repeatedly until moved_links returns 0 and deleted_duplicate_authors returns 0.
-- If lock wait timeout occurs, run again when the backend is stopped, or reduce
-- LIMIT 100 to 25.

CREATE TEMPORARY TABLE duplicate_author_batch AS
SELECT a.author_id AS duplicate_author_id,
       keepers.keep_author_id
FROM authors a
JOIN (
  SELECT MIN(author_id) AS keep_author_id, author_name
  FROM authors
  WHERE author_name IS NOT NULL AND TRIM(author_name) <> ''
  GROUP BY author_name
  HAVING COUNT(*) > 1
) keepers ON keepers.author_name = a.author_name
WHERE a.author_id <> keepers.keep_author_id
ORDER BY a.author_id
LIMIT 25;

UPDATE IGNORE paper_authors pa
JOIN duplicate_author_batch batch ON batch.duplicate_author_id = pa.author_id
SET pa.author_id = batch.keep_author_id;

SELECT ROW_COUNT() AS moved_links;

DELETE a
FROM authors a
JOIN duplicate_author_batch batch ON batch.duplicate_author_id = a.author_id
LEFT JOIN paper_authors pa ON pa.author_id = a.author_id
WHERE pa.author_id IS NULL;

SELECT ROW_COUNT() AS deleted_duplicate_authors;

DROP TEMPORARY TABLE duplicate_author_batch;
