USE research_mysql2;

-- New Procedures and triggers for the existing database
-- Drop ALL old triggers
DROP TRIGGER IF EXISTS trg_after_paper_insert;
DROP TRIGGER IF EXISTS trg_after_paper_authors_insert;
DROP TRIGGER IF EXISTS trg_after_paper_authors_delete;
DROP TRIGGER IF EXISTS trg_before_author_delete;
DROP TRIGGER IF EXISTS trg_after_paper_delete;

-- Drop ALL old procedures
DROP PROCEDURE IF EXISTS CreatePaperWithAuthors;
DROP PROCEDURE IF EXISTS GetTopAuthors;
DROP PROCEDURE IF EXISTS SearchPapersByFilters;
DROP PROCEDURE IF EXISTS GetJournalStats;
DROP PROCEDURE IF EXISTS DeleteAuthorSafe;
DROP PROCEDURE IF EXISTS GetTrendingPapers;
DROP PROCEDURE IF EXISTS GetAuthorImpact;
DROP PROCEDURE IF EXISTS GetIncompletePapers;
DROP PROCEDURE IF EXISTS GetActiveUsers;
DROP PROCEDURE IF EXISTS GetAuthorTrackRecord;

-- remove the existing data in paper metrices

TRUNCATE TABLE paper_metrics;

ALTER TABLE papers 
ADD COLUMN is_important BOOLEAN DEFAULT FALSE;

ALTER TABLE journals 
ADD COLUMN paper_count INT DEFAULT 0;

-- recompute paper metrics from scratch
INSERT INTO paper_metrics (paper_id, author_count, abstract_word_count, paper_age)
SELECT 
    p.paper_id,
    COUNT(pa.author_id) AS author_count,
    IF(p.abstract IS NULL, 0,
       1 + LENGTH(p.abstract) - LENGTH(REPLACE(p.abstract, ' ', ''))
    ) AS word_count,
    YEAR(CURDATE()) - IFNULL(p.publish_year, YEAR(CURDATE())) AS paper_age
FROM papers p
LEFT JOIN paper_authors pa ON p.paper_id = pa.paper_id
GROUP BY p.paper_id;

-- Journal Popularity
UPDATE journals j
SET paper_count = (
    SELECT COUNT(*)
    FROM papers p
    WHERE p.journal_id = j.journal_id
);

-- Procedures

-- Trending Papers
DROP PROCEDURE IF EXISTS GetTrendingPapers;
CREATE PROCEDURE GetTrendingPapers(IN p_year INT, IN p_limit INT)
BEGIN
    SELECT 
        p.paper_id,
        p.title,
        p.publish_year AS year,
        j.journal_name,
        pm.author_count
    FROM papers p
    JOIN paper_metrics pm ON p.paper_id = pm.paper_id
    LEFT JOIN journals j ON p.journal_id = j.journal_id
    WHERE p.publish_year >= p_year
    ORDER BY pm.author_count DESC, p.publish_year DESC
    LIMIT p_limit;
END;

-- Author Impact
CREATE PROCEDURE GetAuthorImpact()
BEGIN
    SELECT 
        a.author_id,
        a.author_name,
        COUNT(pa.paper_id) AS total_papers
    FROM authors a
    JOIN paper_authors pa ON a.author_id = pa.author_id
    GROUP BY a.author_id
    ORDER BY total_papers DESC;
END;

-- Author track record for reviewer/conference tooling
CREATE PROCEDURE GetAuthorTrackRecord(IN p_author_name VARCHAR(255))
BEGIN
    SELECT 
        a.author_id,
        a.author_name,
        COUNT(DISTINCT pa.paper_id) AS total_papers,
        MIN(p.publish_year) AS first_year,
        MAX(p.publish_year) AS last_year,
        GROUP_CONCAT(DISTINCT j.journal_name
                     ORDER BY j.journal_name
                     SEPARATOR ', ') AS journals,
        MIN(CASE
              WHEN jr.best_quartile IN ('Q1', 'Q2', 'Q3', 'Q4')
              THEN jr.best_quartile
            END) AS best_quartile,
        GROUP_CONCAT(DISTINCT COALESCE(jr.best_quartile, 'UNRANKED')
                     ORDER BY COALESCE(jr.best_quartile, 'UNRANKED')
                     SEPARATOR ', ') AS quartile_summary,
        SUM(CASE WHEN p.is_important = TRUE THEN 1 ELSE 0 END) AS collaborative_papers,
        (
          SELECT GROUP_CONCAT(DISTINCT co.author_name
                              ORDER BY co.author_name
                              SEPARATOR ', ')
          FROM paper_authors pa_self
          JOIN paper_authors pa_co ON pa_co.paper_id = pa_self.paper_id
          JOIN authors co ON co.author_id = pa_co.author_id
          WHERE pa_self.author_id = a.author_id
            AND co.author_id <> a.author_id
        ) AS co_authors
    FROM authors a
    JOIN paper_authors pa ON a.author_id = pa.author_id
    JOIN papers p ON p.paper_id = pa.paper_id
    LEFT JOIN journals j ON j.journal_id = p.journal_id
    LEFT JOIN journal_rankings jr
           ON LOWER(jr.title) = LOWER(j.journal_name)
    WHERE a.author_name = p_author_name
    GROUP BY a.author_id, a.author_name;
END;


-- Detect Incomplete papers
CREATE PROCEDURE GetIncompletePapers()
BEGIN
    SELECT paper_id, title
    FROM papers
    WHERE abstract IS NULL 
       OR journal_id IS NULL
       OR publish_year IS NULL;
END;

-- User Activity
CREATE PROCEDURE GetActiveUsers()
BEGIN
    SELECT user_id, name, last_login
    FROM users
    ORDER BY last_login DESC;
END;

--Triggers

-- Auto Mark Important Papers
CREATE TRIGGER trg_mark_important_paper
AFTER INSERT ON paper_metrics
FOR EACH ROW
BEGIN
    IF NEW.author_count >= 5 THEN
        UPDATE papers
        SET is_important = TRUE
        WHERE paper_id = NEW.paper_id;
    END IF;
END;

-- Auto Update last login

CREATE TRIGGER trg_update_last_login
BEFORE UPDATE ON users
FOR EACH ROW
BEGIN
    SET NEW.last_login = CURRENT_TIMESTAMP;
END;

-- Prevent Low quality papers
CREATE TRIGGER trg_validate_paper
BEFORE INSERT ON papers
FOR EACH ROW
BEGIN
    IF NEW.title IS NULL OR LENGTH(NEW.title) < 5 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Invalid paper title';
    END IF;
END;

-- Journal Popularity
CREATE TRIGGER trg_update_journal_count
AFTER INSERT ON papers
FOR EACH ROW
BEGIN
    UPDATE journals
    SET paper_count = paper_count + 1
    WHERE journal_id = NEW.journal_id;
END;

-- Manual setting of Mark Important Papers
UPDATE papers p
JOIN paper_metrics pm ON p.paper_id = pm.paper_id
SET p.is_important = TRUE
WHERE pm.author_count >= 5;

-- reconfirm journal popularity
UPDATE journals j
SET paper_count = (
    SELECT COUNT(*)
    FROM papers p
    WHERE p.journal_id = j.journal_id
);

-- Update last login
UPDATE users
SET last_login = CURRENT_TIMESTAMP
WHERE last_login IS NULL;

-- Procedure call

-- Trending Papers
CALL GetTrendingPapers(2020, 10);

-- Author Impact
CALL GetAuthorImpact();

-- Author Track Record
CALL GetAuthorTrackRecord('Example Author');

-- Get Incomplete papers
CALL GetIncompletePapers();

-- User Acitivity
CALL GetActiveUsers();

-- Verification queries

-- Check important papers
SELECT COUNT(*) FROM papers WHERE is_important = TRUE;


-- Check journal popularity
SELECT journal_name, paper_count 
FROM journals
ORDER BY paper_count DESC
LIMIT 10;

-- Check Metrics
SELECT * FROM paper_metrics LIMIT 10;
