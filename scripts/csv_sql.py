import pandas as pd
import mysql.connector
from datetime import datetime
import ast
import numpy as np
import math

# =====================================================
# CONFIG
# =====================================================
CSV_PATH = r"d:\Amrita\Research\DBMS\DBMS PROJECT\metadata_cleaned.csv"

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",   # CHANGE
    "database": "research_mysql2"
}

BATCH_SIZE = 5000

# =====================================================
# LOAD CSV
# =====================================================
print("📥 Loading cleaned CSV...")

df = pd.read_csv(CSV_PATH, low_memory=False)

df = df.drop_duplicates(subset=["paper_id"])

# Remove rows without title
df = df[df["title"].notna()]
df = df[df["title"].astype(str).str.strip() != ""]

print(f"📊 Total valid papers: {len(df)}")

# Replace NaN with None
df = df.replace({np.nan: None})

df["journal"] = df["journal"].astype(str).str.strip()
df["source"] = df["source"].astype(str).str.strip()

# Parse authors safely
def parse_authors(x):
    if x is None:
        return []
    try:
        return ast.literal_eval(x)
    except:
        return []

df["authors"] = df["authors"].apply(parse_authors)

# =====================================================
# CONNECT MYSQL
# =====================================================
conn = mysql.connector.connect(**DB_CONFIG)
cursor = conn.cursor()

print("✅ Connected to MySQL")

# =====================================================
# CLEAR TABLES
# =====================================================
print("🧹 Clearing existing data...")

cursor.execute("SET FOREIGN_KEY_CHECKS=0;")

cursor.execute("TRUNCATE TABLE paper_authors;")
cursor.execute("TRUNCATE TABLE paper_metrics;")
cursor.execute("TRUNCATE TABLE papers;")
cursor.execute("TRUNCATE TABLE authors;")
cursor.execute("TRUNCATE TABLE journals;")
cursor.execute("TRUNCATE TABLE sources;")

cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
conn.commit()

# =====================================================
# INSERT JOURNALS
# =====================================================
print("📚 Inserting journals...")

unique_journals = set(j for j in df["journal"] if j and j != "None")

cursor.executemany(
    "INSERT IGNORE INTO journals (journal_name) VALUES (%s)",
    [(j,) for j in unique_journals]
)
conn.commit()

print(f"   ✔ Inserted {len(unique_journals)} unique journals")

cursor.execute("SELECT journal_id, journal_name FROM journals")
journal_map = {name: jid for jid, name in cursor.fetchall()}

# =====================================================
# INSERT SOURCES
# =====================================================
print("🌍 Inserting sources...")

unique_sources = set(s for s in df["source"] if s and s != "None")

cursor.executemany(
    "INSERT IGNORE INTO sources (source_name) VALUES (%s)",
    [(s,) for s in unique_sources]
)
conn.commit()

print(f"   ✔ Inserted {len(unique_sources)} unique sources")

cursor.execute("SELECT source_id, source_name FROM sources")
source_map = {name: sid for sid, name in cursor.fetchall()}

# =====================================================
# INSERT AUTHORS
# =====================================================
print("👤 Inserting authors...")

all_authors = set()

for author_list in df["authors"]:
    for author in author_list:
        if author.strip():
            all_authors.add(author.strip())

cursor.executemany(
    "INSERT IGNORE INTO authors (author_name) VALUES (%s)",
    [(a,) for a in all_authors]
)
conn.commit()

print(f"   ✔ Inserted {len(all_authors)} unique authors")

cursor.execute("SELECT author_id, author_name FROM authors")
author_map = {name: aid for aid, name in cursor.fetchall()}

# =====================================================
# INSERT PAPERS
# =====================================================
print("📄 Inserting papers...")

paper_data = []

for _, row in df.iterrows():

    publish_year = int(row["year"]) if row["year"] else None

    paper_data.append((
        row["paper_id"],
        row["sha"] if row["sha"] else None,
        row["title"],
        row["abstract"] if row["abstract"] else None,
        publish_year,
        row["doi"] if row["doi"] else None,
        journal_map.get(row["journal"]),
        source_map.get(row["source"]),
        bool(row["is_covid19"]) if row["is_covid19"] else False,
        bool(row["has_full_text"]) if row["has_full_text"] else False
    ))

total_batches = math.ceil(len(paper_data) / BATCH_SIZE)

for i in range(0, len(paper_data), BATCH_SIZE):
    batch_no = i // BATCH_SIZE + 1

    cursor.executemany("""
        INSERT INTO papers
        (paper_id, sha, title, abstract, publish_year, doi,
         journal_id, source_id, is_covid19, has_full_text)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, paper_data[i:i+BATCH_SIZE])

    conn.commit()

    print(f"   ✔ Papers batch {batch_no}/{total_batches} inserted")

# =====================================================
# INSERT PAPER_AUTHORS (DEDUP SAFE)
# =====================================================
print("🔗 Inserting paper-author relationships...")

pa_data = []

for _, row in df.iterrows():

    seen_authors = set()

    for order, author in enumerate(row["authors"], start=1):

        author_clean = author.strip()

        if author_clean in seen_authors:
            continue

        seen_authors.add(author_clean)

        author_id = author_map.get(author_clean)

        if author_id:
            pa_data.append((row["paper_id"], author_id, order))

total_batches = math.ceil(len(pa_data) / BATCH_SIZE)

for i in range(0, len(pa_data), BATCH_SIZE):
    batch_no = i // BATCH_SIZE + 1

    cursor.executemany("""
        INSERT INTO paper_authors
        (paper_id, author_id, author_order)
        VALUES (%s,%s,%s)
    """, pa_data[i:i+BATCH_SIZE])

    conn.commit()

    print(f"   ✔ Paper-Author batch {batch_no}/{total_batches} inserted")

# =====================================================
# INSERT PAPER_METRICS
# =====================================================
print("📊 Inserting paper metrics...")

current_year = datetime.now().year
metrics_data = []

for _, row in df.iterrows():

    publish_year = int(row["year"]) if row["year"] else None
    paper_age = current_year - publish_year if publish_year else None

    metrics_data.append((
        row["paper_id"],
        len(row["authors"]),
        len(str(row["abstract"]).split()) if row["abstract"] else 0,
        paper_age
    ))

total_batches = math.ceil(len(metrics_data) / BATCH_SIZE)

for i in range(0, len(metrics_data), BATCH_SIZE):
    batch_no = i // BATCH_SIZE + 1

    cursor.executemany("""
        INSERT INTO paper_metrics
        (paper_id, author_count, abstract_word_count, paper_age)
        VALUES (%s,%s,%s,%s)
    """, metrics_data[i:i+BATCH_SIZE])

    conn.commit()

    print(f"   ✔ Metrics batch {batch_no}/{total_batches} inserted")

print("\n🎉 SUCCESS! All data normalized and inserted successfully.")

cursor.close()
conn.close()
