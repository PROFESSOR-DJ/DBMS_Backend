USE research_mysql2;

CREATE TABLE IF NOT EXISTS journal_rankings (
    journal_ranking_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    sjr_rank INT,
    title VARCHAR(512) NOT NULL,
    oa BOOLEAN,
    country VARCHAR(128),
    sjr_index DECIMAL(10,3),
    citescore DECIMAL(10,2),
    h_index INT,
    best_quartile VARCHAR(10),
    best_categories JSON,
    best_subject_area VARCHAR(255),
    best_subject_rank VARCHAR(50),
    total_docs INT,
    total_docs_3y INT,
    total_refs INT,
    total_cites_3y INT,
    citable_docs_3y INT,
    cites_per_doc_2y DECIMAL(10,3),
    refs_per_doc DECIMAL(10,3),
    publisher VARCHAR(255),
    core_collection VARCHAR(255),
    coverage VARCHAR(100),
    active BOOLEAN,
    in_press BOOLEAN,
    iso_language_code VARCHAR(10),
    asjc_codes TEXT,
    source_updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_journal_rankings_title (title),
    INDEX idx_journal_rank (sjr_rank),
    INDEX idx_journal_country (country),
    INDEX idx_journal_quartile (best_quartile),
    INDEX idx_journal_sjr (sjr_index)
);

DROP TRIGGER IF EXISTS trg_journal_rankings_seed_on_journal_insert;
DROP TRIGGER IF EXISTS trg_journal_rankings_sync_stub_on_journal_update;

DELIMITER $$

CREATE TRIGGER trg_journal_rankings_seed_on_journal_insert
AFTER INSERT ON journals
FOR EACH ROW
BEGIN
    INSERT INTO journal_rankings (
        title,
        best_quartile,
        active,
        source_updated_at
    )
    VALUES (
        NEW.journal_name,
        'UNRANKED',
        TRUE,
        CURRENT_TIMESTAMP
    )
    ON DUPLICATE KEY UPDATE
        source_updated_at = CURRENT_TIMESTAMP;
END$$

CREATE TRIGGER trg_journal_rankings_sync_stub_on_journal_update
AFTER UPDATE ON journals
FOR EACH ROW
BEGIN
    IF NEW.journal_name <> OLD.journal_name THEN
        UPDATE journal_rankings
        SET title = NEW.journal_name,
            source_updated_at = CURRENT_TIMESTAMP
        WHERE title = OLD.journal_name
          AND COALESCE(best_quartile, 'UNRANKED') = 'UNRANKED'
          AND sjr_rank IS NULL
          AND sjr_index IS NULL
          AND citescore IS NULL
          AND h_index IS NULL;

        INSERT INTO journal_rankings (
            title,
            best_quartile,
            active,
            source_updated_at
        )
        VALUES (
            NEW.journal_name,
            'UNRANKED',
            TRUE,
            CURRENT_TIMESTAMP
        )
        ON DUPLICATE KEY UPDATE
            source_updated_at = CURRENT_TIMESTAMP;
    END IF;
END$$

DELIMITER ;
