USE research_mysql2;

-- =====================================================
-- SECTION 1: STORED PROCEDURES
-- =====================================================

DROP PROCEDURE IF EXISTS CreatePaperWithAuthors;
DROP PROCEDURE IF EXISTS GetTopAuthors;
DROP PROCEDURE IF EXISTS SearchPapersByFilters;
DROP PROCEDURE IF EXISTS GetJournalStats;
DROP PROCEDURE IF EXISTS DeleteAuthorSafe;

DELIMITER $$


-- Procedure 1: CreatePaperWithAuthors
-- Inserts a paper + its metrics + authors in one
-- atomic transaction.  On duplicate paper_id it rolls
-- back and signals a clean error.

CREATE PROCEDURE CreatePaperWithAuthors(
    IN  p_paper_id       VARCHAR(20),
    IN  p_title          TEXT,
    IN  p_abstract       LONGTEXT,
    IN  p_publish_year   INT,
    IN  p_doi            VARCHAR(255),
    IN  p_journal_name   VARCHAR(255),
    IN  p_is_covid19     BOOLEAN,
    IN  p_has_full_text  BOOLEAN,
    IN  p_authors_csv    TEXT,      
    OUT p_status         VARCHAR(50),
    OUT p_message        VARCHAR(255)
)
proc_label: BEGIN
    DECLARE v_journal_id    INT DEFAULT NULL;
    DECLARE v_source_id     INT DEFAULT NULL;
    DECLARE v_author_id     INT;
    DECLARE v_author_name   VARCHAR(255);
    DECLARE v_author_order  INT DEFAULT 1;
    DECLARE v_remaining     TEXT;
    DECLARE v_pos           INT;
    DECLARE v_paper_age     INT;
    DECLARE v_word_count    INT;
    DECLARE v_author_count  INT DEFAULT 0;
    DECLARE v_exit_handler  BOOLEAN DEFAULT FALSE;

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1
            @sql_state = RETURNED_SQLSTATE,
            @err_msg   = MESSAGE_TEXT;
        ROLLBACK;
        SET p_status  = 'ERROR';
        SET p_message = @err_msg;
    END;

    -- Count authors from CSV
    IF p_authors_csv IS NOT NULL AND p_authors_csv != '' THEN
        SET v_author_count = 1 + (LENGTH(p_authors_csv) - LENGTH(REPLACE(p_authors_csv, ',', '')));
    END IF;

    -- Compute derived metrics
    SET v_paper_age  = YEAR(CURDATE()) - IFNULL(p_publish_year, YEAR(CURDATE()));
    SET v_word_count = IF(p_abstract IS NULL OR p_abstract = '', 0,
                          1 + LENGTH(TRIM(p_abstract)) - LENGTH(REPLACE(TRIM(p_abstract), ' ', '')));

    START TRANSACTION;

    -- ── Resolve or create journal ──
    IF p_journal_name IS NOT NULL AND p_journal_name != '' THEN
        SELECT journal_id INTO v_journal_id
        FROM journals WHERE journal_name = p_journal_name LIMIT 1;

        IF v_journal_id IS NULL THEN
            INSERT INTO journals (journal_name) VALUES (p_journal_name);
            SET v_journal_id = LAST_INSERT_ID();
        END IF;
    END IF;

    -- ── Insert paper ──
    INSERT INTO papers
        (paper_id, title, abstract, publish_year, doi, journal_id, source_id, is_covid19, has_full_text)
    VALUES
        (p_paper_id, p_title, p_abstract, p_publish_year, p_doi, v_journal_id, v_source_id, p_is_covid19, p_has_full_text);

    -- ── Insert paper_metrics ──
    -- (The AFTER INSERT trigger also fires here, but we do an explicit
    --  INSERT so the procedure is self-contained and testable standalone.)
    INSERT IGNORE INTO paper_metrics (paper_id, author_count, abstract_word_count, paper_age)
    VALUES (p_paper_id, v_author_count, v_word_count, v_paper_age);

    -- ── Insert authors from CSV ──
    SET v_remaining = p_authors_csv;

    WHILE v_remaining IS NOT NULL AND v_remaining != '' DO
        SET v_pos = LOCATE(',', v_remaining);

        IF v_pos > 0 THEN
            SET v_author_name = TRIM(SUBSTRING(v_remaining, 1, v_pos - 1));
            SET v_remaining   = TRIM(SUBSTRING(v_remaining, v_pos + 1));
        ELSE
            SET v_author_name = TRIM(v_remaining);
            SET v_remaining   = '';
        END IF;

        IF v_author_name != '' THEN
            -- Find or create author
            SELECT author_id INTO v_author_id
            FROM authors WHERE author_name = v_author_name LIMIT 1;

            IF v_author_id IS NULL THEN
                INSERT INTO authors (author_name) VALUES (v_author_name);
                SET v_author_id = LAST_INSERT_ID();
            END IF;

            -- Link paper ↔ author
            INSERT IGNORE INTO paper_authors (paper_id, author_id, author_order)
            VALUES (p_paper_id, v_author_id, v_author_order);

            SET v_author_order = v_author_order + 1;
            SET v_author_id    = NULL;
        END IF;
    END WHILE;

    COMMIT;

    SET p_status  = 'SUCCESS';
    SET p_message = CONCAT('Paper ', p_paper_id, ' created with ', v_author_order - 1, ' author(s).');
END$$


-- ─────────────────────────────────────────────────────
-- Procedure 2: GetTopAuthors
-- Returns the top N authors ranked by paper count.
-- Mirrors the JS heuristic optimisation: GROUP BY on
-- the junction table first, then JOIN to author names.
-- ─────────────────────────────────────────────────────
CREATE PROCEDURE GetTopAuthors(
    IN p_limit INT
)
BEGIN
    DECLARE v_limit INT DEFAULT 10;

    -- Guard: clamp limit to a sane range
    IF p_limit IS NULL OR p_limit < 1 THEN
        SET v_limit = 10;
    ELSEIF p_limit > 1000 THEN
        SET v_limit = 1000;
    ELSE
        SET v_limit = p_limit;
    END IF;

    -- Heuristic optimisation:
    --   Step 1 – COUNT on the narrow junction table (no text columns).
    --   Step 2 – JOIN to authors only for the top-N rows.
    SELECT
        a.author_id,
        a.author_name  AS name,
        agg.paper_count
    FROM (
        SELECT   author_id, COUNT(paper_id) AS paper_count
        FROM     paper_authors
        GROUP BY author_id
        ORDER BY paper_count DESC
        LIMIT    v_limit
    ) AS agg
    JOIN authors a ON a.author_id = agg.author_id
    ORDER BY agg.paper_count DESC;
END$$


-- ─────────────────────────────────────────────────────
-- Procedure 3: SearchPapersByFilters
-- Full-featured paper search used by the frontend.
-- Supports year range, journal substring, and keyword
-- (title LIKE).  Returns paginated results + total.
-- ─────────────────────────────────────────────────────
CREATE PROCEDURE SearchPapersByFilters(
    IN  p_query        VARCHAR(500),   -- title keyword (NULL = all)
    IN  p_year_from    INT,            -- NULL = no lower bound
    IN  p_year_to      INT,            -- NULL = no upper bound
    IN  p_journal      VARCHAR(255),   -- NULL = all journals
    IN  p_limit        INT,            -- rows per page  (default 20)
    IN  p_offset       INT             -- pagination offset (default 0)
)
BEGIN
    DECLARE v_limit  INT DEFAULT 20;
    DECLARE v_offset INT DEFAULT 0;

    IF p_limit  IS NULL OR p_limit  < 1  THEN SET v_limit  = 20;   END IF;
    IF p_limit  > 200                    THEN SET v_limit  = 200;  END IF;
    IF p_offset IS NULL OR p_offset < 0  THEN SET v_offset = 0;    END IF;
    SET v_limit  = p_limit;
    SET v_offset = p_offset;

    -- Dynamic WHERE is emulated with COALESCE / OR-NULL trick
    -- so no prepared statement is needed inside the procedure.
    SELECT
        p.paper_id,
        p.title,
        p.abstract,
        p.publish_year                                              AS year,
        p.doi,
        j.journal_name                                             AS journal,
        p.is_covid19,
        p.has_full_text,
        GROUP_CONCAT(a.author_name ORDER BY pa.author_order SEPARATOR ', ') AS authors
    FROM   papers p
    LEFT JOIN journals      j  ON  j.journal_id  = p.journal_id
    LEFT JOIN paper_authors pa ON pa.paper_id     = p.paper_id
    LEFT JOIN authors       a  ON  a.author_id    = pa.author_id
    WHERE
        (p_query     IS NULL OR p.title          LIKE CONCAT('%', p_query,   '%'))
    AND (p_year_from IS NULL OR p.publish_year  >= p_year_from)
    AND (p_year_to   IS NULL OR p.publish_year  <= p_year_to)
    AND (p_journal   IS NULL OR j.journal_name  LIKE CONCAT('%', p_journal, '%'))
    GROUP BY
        p.paper_id, p.title, p.abstract, p.publish_year,
        p.doi, j.journal_name, p.is_covid19, p.has_full_text
    ORDER BY p.publish_year DESC, p.title ASC
    LIMIT  v_limit
    OFFSET v_offset;
END$$


-- ─────────────────────────────────────────────────────
-- Procedure 4: GetJournalStats
-- Aggregated stats per journal: paper count, year span,
-- covid paper count.  Used by the stats dashboard.
-- ─────────────────────────────────────────────────────
CREATE PROCEDURE GetJournalStats(
    IN p_limit INT
)
BEGIN
    DECLARE v_limit INT DEFAULT 50;
    IF p_limit IS NULL OR p_limit < 1 THEN SET v_limit = 50; END IF;
    IF p_limit > 500                  THEN SET v_limit = 500; END IF;
    SET v_limit = p_limit;

    SELECT
        j.journal_name                         AS journal,
        COUNT(p.paper_id)                      AS paper_count,
        MIN(p.publish_year)                    AS first_year,
        MAX(p.publish_year)                    AS last_year,
        SUM(p.is_covid19)                      AS covid_papers,
        ROUND(AVG(pm.abstract_word_count), 1)  AS avg_abstract_words
    FROM   journals j
    JOIN   papers p  ON p.journal_id  = j.journal_id
    LEFT JOIN paper_metrics pm ON pm.paper_id = p.paper_id
    GROUP BY j.journal_id, j.journal_name
    ORDER BY paper_count DESC
    LIMIT v_limit;
END$$


-- ─────────────────────────────────────────────────────
-- Procedure 5: DeleteAuthorSafe
-- Deletes an author only if they have no linked papers.
-- Returns a status so the caller knows what happened.
-- ─────────────────────────────────────────────────────
CREATE PROCEDURE DeleteAuthorSafe(
    IN  p_author_id INT,
    OUT p_status    VARCHAR(50),
    OUT p_message   VARCHAR(255)
)
BEGIN
    DECLARE v_paper_count INT DEFAULT 0;
    DECLARE v_author_name VARCHAR(255) DEFAULT '';

    DECLARE EXIT HANDLER FOR SQLEXCEPTION
    BEGIN
        GET DIAGNOSTICS CONDITION 1 @err_msg = MESSAGE_TEXT;
        SET p_status  = 'ERROR';
        SET p_message = @err_msg;
    END;

    -- Verify author exists
    SELECT author_name INTO v_author_name
    FROM authors WHERE author_id = p_author_id LIMIT 1;

    IF v_author_name IS NULL OR v_author_name = '' THEN
        SET p_status  = 'NOT_FOUND';
        SET p_message = CONCAT('Author with id ', p_author_id, ' does not exist.');
        LEAVE proc_label;
    END IF;

    -- Count linked papers
    SELECT COUNT(*) INTO v_paper_count
    FROM paper_authors WHERE author_id = p_author_id;

    IF v_paper_count > 0 THEN
        SET p_status  = 'BLOCKED';
        SET p_message = CONCAT('Author "', v_author_name, '" has ', v_paper_count,
                               ' linked paper(s). Unlink them before deleting.');
    ELSE
        DELETE FROM authors WHERE author_id = p_author_id;
        SET p_status  = 'DELETED';
        SET p_message = CONCAT('Author "', v_author_name, '" deleted successfully.');
    END IF;
END$$

DELIMITER ;


-- =====================================================
-- SECTION 2: TRIGGERS
-- =====================================================

DROP TRIGGER IF EXISTS trg_after_paper_insert;
DROP TRIGGER IF EXISTS trg_after_paper_authors_insert;
DROP TRIGGER IF EXISTS trg_after_paper_authors_delete;
DROP TRIGGER IF EXISTS trg_before_author_delete;
DROP TRIGGER IF EXISTS trg_after_paper_delete;

DELIMITER $$

-- ─────────────────────────────────────────────────────
-- Trigger 1: trg_after_paper_insert
-- Automatically inserts a paper_metrics row whenever a
-- new paper is added.  Computes paper_age and
-- abstract_word_count so application code never needs
-- to remember to do it.
-- ─────────────────────────────────────────────────────
CREATE TRIGGER trg_after_paper_insert
AFTER INSERT ON papers
FOR EACH ROW
BEGIN
    DECLARE v_word_count INT DEFAULT 0;
    DECLARE v_paper_age  INT DEFAULT 0;

    -- Word count: count spaces + 1  (fast approximation)
    IF NEW.abstract IS NOT NULL AND NEW.abstract != '' THEN
        SET v_word_count = 1 + LENGTH(TRIM(NEW.abstract))
                             - LENGTH(REPLACE(TRIM(NEW.abstract), ' ', ''));
    END IF;

    -- Paper age in full years
    SET v_paper_age = YEAR(CURDATE()) - IFNULL(NEW.publish_year, YEAR(CURDATE()));

    -- Use INSERT IGNORE so the procedure path (which also inserts)
    -- does not cause a duplicate-key error.
    INSERT IGNORE INTO paper_metrics
        (paper_id, author_count, abstract_word_count, paper_age)
    VALUES
        (NEW.paper_id, 0, v_word_count, v_paper_age);
END$$


-- ─────────────────────────────────────────────────────
-- Trigger 2: trg_after_paper_authors_insert
-- Keeps paper_metrics.author_count in sync whenever a
-- new paper↔author link is created.
-- ─────────────────────────────────────────────────────
CREATE TRIGGER trg_after_paper_authors_insert
AFTER INSERT ON paper_authors
FOR EACH ROW
BEGIN
    UPDATE paper_metrics
    SET    author_count = (
               SELECT COUNT(*)
               FROM   paper_authors
               WHERE  paper_id = NEW.paper_id
           )
    WHERE  paper_id = NEW.paper_id;
END$$


-- ─────────────────────────────────────────────────────
-- Trigger 3: trg_after_paper_authors_delete
-- Keeps paper_metrics.author_count in sync when a
-- paper↔author link is removed.
-- ─────────────────────────────────────────────────────
CREATE TRIGGER trg_after_paper_authors_delete
AFTER DELETE ON paper_authors
FOR EACH ROW
BEGIN
    UPDATE paper_metrics
    SET    author_count = (
               SELECT COUNT(*)
               FROM   paper_authors
               WHERE  paper_id = OLD.paper_id
           )
    WHERE  paper_id = OLD.paper_id;
END$$


-- ─────────────────────────────────────────────────────
-- Trigger 4: trg_before_author_delete
-- Guards against deleting an author that still has
-- linked papers.  Raises a descriptive error so the
-- application receives a clear message instead of a
-- generic FK violation.
-- ─────────────────────────────────────────────────────
CREATE TRIGGER trg_before_author_delete
BEFORE DELETE ON authors
FOR EACH ROW
BEGIN
    DECLARE v_count INT DEFAULT 0;

    SELECT COUNT(*) INTO v_count
    FROM   paper_authors
    WHERE  author_id = OLD.author_id;

    IF v_count > 0 THEN
        SIGNAL SQLSTATE '45000'
        SET MESSAGE_TEXT = 'Cannot delete author: still linked to one or more papers. Remove paper links first.';
    END IF;
END$$


-- ─────────────────────────────────────────────────────
-- Trigger 5: trg_after_paper_delete
-- Cleans up paper_metrics rows when a paper is hard-
-- deleted (cascade FK would handle paper_authors, but
-- paper_metrics uses ON DELETE CASCADE already; this
-- trigger logs the deletion for audit purposes).
-- ─────────────────────────────────────────────────────
CREATE TRIGGER trg_after_paper_delete
AFTER DELETE ON papers
FOR EACH ROW
BEGIN
    -- paper_metrics is cleaned by CASCADE FK, nothing extra
    -- needed there.  This trigger exists to demonstrate AFTER
    -- DELETE logic — extend with audit-log INSERT if required.
    -- Example audit insert (table must exist):
    -- INSERT INTO audit_log (action, entity, entity_id, happened_at)
    -- VALUES ('DELETE', 'paper', OLD.paper_id, NOW());
    BEGIN END;  -- no-op placeholder; safe to extend
END$$

DELIMITER ;


-- =====================================================
-- SECTION 3: QUICK SMOKE-TEST
-- Call procedures to verify they parse and execute.
-- =====================================================

-- Test GetTopAuthors
CALL GetTopAuthors(5);

-- Test SearchPapersByFilters (all papers, first page)
CALL SearchPapersByFilters(NULL, NULL, NULL, NULL, 5, 0);

-- Test GetJournalStats
CALL GetJournalStats(5);