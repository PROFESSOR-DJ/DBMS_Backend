-- =====================================================
-- DBMS PROJECT
-- Research Paper Management System
-- Database: research_sql
-- =====================================================

-- =====================================================
-- 1. CREATE DATABASE
-- =====================================================
DROP DATABASE IF EXISTS research_sql;
CREATE DATABASE research_sql;
USE research_sql;

-- =====================================================
-- 2. USERS TABLE (userModel.js)
-- =====================================================
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    role ENUM('admin', 'researcher', 'viewer') DEFAULT 'viewer',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 3. AUTHORS TABLE (authorModel.js)
-- =====================================================
CREATE TABLE authors (
    author_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    affiliation VARCHAR(255),
    email VARCHAR(150),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =====================================================
-- 4. PAPERS TABLE (paperModel.js)
-- =====================================================
CREATE TABLE papers (
    paper_id INT AUTO_INCREMENT PRIMARY KEY,
    title TEXT NOT NULL,
    abstract TEXT,
    year INT,
    type VARCHAR(50),
    journal VARCHAR(255),
    booktitle VARCHAR(255),
    volume VARCHAR(50),
    number VARCHAR(50),
    pages VARCHAR(50),
    url TEXT,
    created_by INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (created_by) REFERENCES users(user_id)
        ON DELETE SET NULL
);

-- =====================================================
-- 5. PAPER_AUTHORS TABLE (paperAuthorModel.js)
-- Many-to-Many Relationship
-- =====================================================
CREATE TABLE paper_authors (
    paper_id INT,
    author_id INT,
    author_order INT,
    PRIMARY KEY (paper_id, author_id),

    FOREIGN KEY (paper_id) REFERENCES papers(paper_id)
        ON DELETE CASCADE,

    FOREIGN KEY (author_id) REFERENCES authors(author_id)
        ON DELETE CASCADE
);

-- =====================================================
-- 6. INDEXES (Performance Optimization)
-- =====================================================
CREATE INDEX idx_user_email ON users(email);
CREATE INDEX idx_author_name ON authors(name);
CREATE INDEX idx_paper_year ON papers(year);
CREATE INDEX idx_paper_type ON papers(type);

-- =====================================================
-- 7. VIEW (For Reports & Viva)
-- =====================================================
CREATE VIEW paper_with_authors AS
SELECT 
    p.paper_id,
    p.title,
    p.year,
    GROUP_CONCAT(a.name ORDER BY pa.author_order SEPARATOR ', ') AS authors
FROM papers p
JOIN paper_authors pa ON p.paper_id = pa.paper_id
JOIN authors a ON pa.author_id = a.author_id
GROUP BY p.paper_id;

USE research_sql;

SET GLOBAL local_infile = 1;

DROP TABLE IF EXISTS temp_metadata;

CREATE TABLE temp_metadata (
    paper_id INT,
    title TEXT,
    year VARCHAR(10),
    journal VARCHAR(255),
    authors TEXT
);






LOAD DATA LOCAL INFILE 'D:/Amrita/Research/DBMS/DBMS PROJECT/metadata_cleaned.csv'
INTO TABLE temp_metadata
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(paper_id, title, year, journal, authors);


INSERT INTO users (name, email, password, role)
VALUES 
('Admin User', 'admin@research.com', 'hashed_password', 'admin'),
('Alice Researcher', 'alice@university.edu', 'hashed_password', 'researcher'),
('Bob Viewer', 'bob@gmail.com', 'hashed_password', 'viewer');


INSERT INTO authors (name, affiliation, email)
VALUES
('Dr. John Smith', 'MIT', 'john@mit.edu'),
('Dr. Emily Clark', 'Stanford University', 'emily@stanford.edu'),
('Dr. Raj Kumar', 'IIT Madras', 'raj@iitm.ac.in'),
('Dr. Sarah Lee', 'Oxford University', 'sarah@oxford.ac.uk');


INSERT INTO papers 
(title, abstract, year, type, journal, booktitle, volume, number, pages, url, created_by)
VALUES
(
 'Artificial Intelligence in Healthcare',
 'Study on AI applications in medical diagnosis.',
 2023,
 'article',
 'IEEE Transactions on AI',
 NULL,
 '12',
 '3',
 '101-120',
 'https://ieee.org/ai-healthcare',
 1
),
(
 'Deep Learning for Cancer Detection',
 'Using CNN models for early cancer detection.',
 2022,
 'article',
 'Springer Medical Journal',
 NULL,
 '8',
 '2',
 '45-60',
 'https://springer.com/cancer-dl',
 2
),
(
 'Big Data Analytics in Education',
 'Educational data mining techniques and performance prediction.',
 2021,
 'conference',
 NULL,
 'ICDE 2021',
 NULL,
 NULL,
 '200-210',
 'https://icde2021.org/paper123',
 2
);

-- Paper 1
INSERT INTO paper_authors VALUES (1, 1, 1);
INSERT INTO paper_authors VALUES (1, 2, 2);

-- Paper 2
INSERT INTO paper_authors VALUES (2, 2, 1);
INSERT INTO paper_authors VALUES (2, 3, 2);

-- Paper 3
INSERT INTO paper_authors VALUES (3, 3, 1);
INSERT INTO paper_authors VALUES (3, 4, 2);

SELECT * FROM paper_with_authors;
