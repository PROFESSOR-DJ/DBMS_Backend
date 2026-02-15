import pandas as pd
import mysql.connector
import ast
import os
import sys
import time
from datetime import datetime
import numpy as np

# Configuration
CSV_PATH = r"d:\Amrita\Research\DBMS\DBMS PROJECT\metadata_cleaned.csv"
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'research_mysql2',
    'port': 3306
}

def connect_db():
    return mysql.connector.connect(**DB_CONFIG)

def safe_literal_eval(val):
    if pd.isna(val) or val == "":
        return []
    try:
        return ast.literal_eval(val)
    except (ValueError, SyntaxError):
        return []

def execute_batch_with_retry(cursor, query, data, max_retries=3):
    if not data:
        return
    for attempt in range(max_retries):
        try:
            cursor.executemany(query, data)
            return
        except mysql.connector.errors.InternalError as e:
            if e.errno == 1213:  # Deadlock
                if attempt < max_retries - 1:
                    print(f"   Deadlock detected. Retrying batch (attempt {attempt+1}/{max_retries})...")
                    time.sleep(1)
                    continue
            raise e
        except Exception as e:
            raise e

def migrate():
    print("Starting migration...")
    
    # 1. Connect to DB
    try:
        conn = connect_db()
        cursor = conn.cursor()
        print("Connected to database.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    # 2. Load CSV
    print(f"Loading CSV from {CSV_PATH}...")
    try:
        df = pd.read_csv(CSV_PATH, low_memory=False)
        print(f"Loaded {len(df)} rows.")
    except FileNotFoundError:
        print(f"CSV file not found at {CSV_PATH}")
        return

    # Pre-processing
    print("Pre-processing data...")
    df['authors_list'] = df['authors'].apply(safe_literal_eval)
    
    df['journal'] = df['journal'].fillna('Unknown Journal').astype(str)
    df['source'] = df['source'].fillna('Unknown Source').astype(str)
    df['year'] = pd.to_numeric(df['year'], errors='coerce').fillna(0).astype(int)
    
    # --- 3. Normalization: Sources ---
    print("Normalizing Sources...")
    unique_sources = set()
    for s in df['source'].unique():
        if s: unique_sources.add(s[:100])
            
    source_data = [(s,) for s in unique_sources]
    execute_batch_with_retry(
        cursor,
        "INSERT INTO sources (source_name) VALUES (%s) ON DUPLICATE KEY UPDATE source_name=source_name",
        list(source_data)
    )
    conn.commit()
    
    cursor.execute("SELECT source_name, source_id FROM sources")
    source_map = {name: id for name, id in cursor.fetchall()}
    print(f"Processed {len(source_map)} sources.")

    # --- 4. Normalization: Journals ---
    print("Normalizing Journals...")
    unique_journals = set()
    for j in df['journal'].unique():
        if j: unique_journals.add(j[:255])
            
    journal_data = [(j,) for j in unique_journals]
    
    BATCH_SIZE = 500  # Reduced batch size
    for i in range(0, len(journal_data), BATCH_SIZE):
        batch = journal_data[i:i+BATCH_SIZE]
        execute_batch_with_retry(
            cursor,
            "INSERT INTO journals (journal_name) VALUES (%s) ON DUPLICATE KEY UPDATE journal_name=journal_name",
            batch
        )
        conn.commit()
        
    cursor.execute("SELECT journal_name, journal_id FROM journals")
    journal_map = {name: id for name, id in cursor.fetchall()}
    print(f"Processed {len(journal_map)} journals.")

    # --- 5. Normalization: Authors ---
    print("Normalizing Authors (this map take a while)...")
    all_authors = set()
    for authors in df['authors_list']:
        for a in authors:
            if a: all_authors.add(a[:255])
    
    print(f"Found {len(all_authors)} unique authors.")
    author_list = list(all_authors)
    author_data = [(a,) for a in author_list]
    
    for i in range(0, len(author_data), BATCH_SIZE):
        batch = author_data[i:i+BATCH_SIZE]
        execute_batch_with_retry(
            cursor,
            "INSERT INTO authors (author_name) VALUES (%s) ON DUPLICATE KEY UPDATE author_name=author_name",
            batch
        )
        if i % 10000 == 0:
            conn.commit()
            print(f"   Inserted {i}/{len(author_data)} authors...", end='\r')
    conn.commit()
    print("\nAuthors inserted.")

    print("Building author map...")
    # This might use a lot of memory. If it fails, we need chunked lookups.
    cursor.execute("SELECT author_name, author_id FROM authors")
    author_map = {name: id for name, id in cursor.fetchall()}
    print(f"Author map built ({len(author_map)} entries).")

    # --- 6. Insert Papers ---
    print("Inserting Papers...")
    
    paper_batch = []
    paper_metrics_batch = []
    paper_authors_batch = []
    
    current_year = datetime.now().year
    total_len = len(df)
    
    for idx, row in df.iterrows():
        s_val = str(row['source'])[:100]
        j_val = str(row['journal'])[:255]
        
        s_id = source_map.get(s_val)
        j_id = journal_map.get(j_val)
        
        p_id = str(row['paper_id'])[:20]
        title = row['title'] if pd.notna(row['title']) else "No Title"
        abstract = row['abstract'] if pd.notna(row['abstract']) else ""
        sha = row['sha'] if pd.notna(row['sha']) else None
        if sha: sha = sha[:64]
        doi = row['doi'] if pd.notna(row['doi']) else None
        if doi: doi = doi[:255]
        year = int(row['year'])
        is_covid = bool(row['is_covid19'])
        has_full = bool(row['has_full_text'])
        
        auth_list = row['authors_list']
        auth_count = len(auth_list)
        word_count = len(abstract.split()) if abstract else 0
        age = current_year - year
        
        paper_batch.append((
            p_id, sha, title, abstract, year, doi, j_id, s_id, is_covid, has_full
        ))
        
        paper_metrics_batch.append((
            p_id, auth_count, word_count, age
        ))
        
        for order, author_name in enumerate(auth_list):
            trunc_name = author_name[:255]
            if trunc_name in author_map:
                a_id = author_map[trunc_name]
                paper_authors_batch.append((p_id, a_id, order + 1))

        if len(paper_batch) >= BATCH_SIZE:
            try:
                execute_batch_with_retry(
                    cursor,
                    """INSERT IGNORE INTO papers 
                    (paper_id, sha, title, abstract, publish_year, doi, journal_id, source_id, is_covid19, has_full_text)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
                    paper_batch
                )
                
                execute_batch_with_retry(
                    cursor,
                    """INSERT IGNORE INTO paper_metrics
                    (paper_id, author_count, abstract_word_count, paper_age)
                    VALUES (%s, %s, %s, %s)""",
                    paper_metrics_batch
                )
                
                execute_batch_with_retry(
                    cursor,
                    """INSERT IGNORE INTO paper_authors
                    (paper_id, author_id, author_order)
                    VALUES (%s, %s, %s)""",
                    paper_authors_batch
                )
                
                conn.commit()
            except Exception as e:
                print(f"\nError processing batch at index {idx}: {e}")
                # Optional: continue or break? 
                # continue to try next batch might be better for bulk import
            
            paper_batch = []
            paper_metrics_batch = []
            paper_authors_batch = []
            
            print(f"   Processed {idx}/{total_len} papers...", end='\r')

    # Insert remaining
    if paper_batch:
        execute_batch_with_retry(
            cursor,
            """INSERT IGNORE INTO papers 
            (paper_id, sha, title, abstract, publish_year, doi, journal_id, source_id, is_covid19, has_full_text)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", 
            paper_batch
        )
        execute_batch_with_retry(
             cursor,
            """INSERT IGNORE INTO paper_metrics
            (paper_id, author_count, abstract_word_count, paper_age)
            VALUES (%s, %s, %s, %s)""",
            paper_metrics_batch
        )
        execute_batch_with_retry(
            cursor,
            """INSERT IGNORE INTO paper_authors
            (paper_id, author_id, author_order)
            VALUES (%s, %s, %s)""",
            paper_authors_batch
        )
        conn.commit()

    print(f"\nMigration completed successfully!")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    migrate()
