DROP DATABASE research_mysql2;


CREATE DATABASE research_mysql2;
USE research_mysql2;

-- =========================================
-- 1️⃣ JOURNALS (must come before papers)
-- =========================================
CREATE TABLE journals (
    journal_id INT AUTO_INCREMENT PRIMARY KEY,
    journal_name VARCHAR(255) UNIQUE NOT NULL,
    impact_factor DECIMAL(4,2),
    publisher VARCHAR(255)
);

-- =========================================
-- 2️⃣ SOURCES (must come before papers)
-- =========================================
CREATE TABLE sources (
    source_id INT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(100) UNIQUE NOT NULL
);

-- =========================================
-- 3️⃣ PAPERS
-- =========================================
CREATE TABLE papers (
    paper_id VARCHAR(20) PRIMARY KEY,
    sha TEXT,  -- fixed here directly
    title TEXT NOT NULL,
    abstract LONGTEXT,
    publish_year INT,
    doi VARCHAR(255),
    journal_id INT,
    source_id INT,
    is_covid19 BOOLEAN DEFAULT FALSE,
    has_full_text BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (journal_id) REFERENCES journals(journal_id),
    FOREIGN KEY (source_id) REFERENCES sources(source_id)
);

-- =========================================
-- 4️⃣ AUTHORS
-- =========================================
CREATE TABLE authors (
    author_id INT AUTO_INCREMENT PRIMARY KEY,
    author_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================================
-- 5️⃣ PAPER_AUTHORS (Many-to-Many)
-- =========================================
CREATE TABLE paper_authors (
    paper_id VARCHAR(20),
    author_id INT,
    author_order INT,
    PRIMARY KEY (paper_id, author_id),
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id) ON DELETE CASCADE,
    FOREIGN KEY (author_id) REFERENCES authors(author_id) ON DELETE CASCADE
);

-- =========================================
-- 6️⃣ PAPER METRICS (Derived Attributes)
-- =========================================
CREATE TABLE paper_metrics (
    paper_id VARCHAR(20) PRIMARY KEY,
    author_count INT,
    abstract_word_count INT,
    paper_age INT,
    FOREIGN KEY (paper_id) REFERENCES papers(paper_id) ON DELETE CASCADE
);


CREATE TABLE users (
  user_id INT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  password VARCHAR(255) NOT NULL,
  role VARCHAR(50) DEFAULT 'researcher',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  last_login TIMESTAMP
);
