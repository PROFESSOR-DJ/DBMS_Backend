# Neo4j export script — streaming, memory-safe, single-process
#
# Root causes fixed vs the previous version:
#   1. No multiprocessing — IPC queues cannot handle 5M+ row results (MemoryError)
#   2. No fetchall() on large tables — every big query streams row-by-row
#   3. Co-authorship NOT computed by SQL self-join — that join on 5.4M rows
#      produces billions of intermediate rows and OOMs both MySQL and the caller.
#      Instead we stream paper_authors ordered by paper_id and compute pairs
#      in Python, which is O(N) in memory relative to unique pairs found.
#   4. Fixed typo: r["a   author_name"] → r["author_name"]
#   5. MongoDB aggregations use allowDiskUse=True so they don't OOM Mongo
#      on large keyword collections.
#
# Run:
#   pip install mysql-connector-python pymongo python-dotenv tqdm
#   python scripts/sqltoneo.py
#
# Output: scripts/neo4j_export/*.csv  (ready to copy to Neo4j import folder)

import os
import csv
import re
import time
from pathlib import Path
from collections import defaultdict
from dotenv import load_dotenv
import mysql.connector
from pymongo import MongoClient
from tqdm import tqdm

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST",     "localhost"),
    "port":     int(os.getenv("MYSQL_PORT", 3306)),
    "user":     os.getenv("MYSQL_USER",     "root"),
    "password": os.getenv("MYSQL_PASSWORD", "root"),
    "database": os.getenv("MYSQL_DATABASE", "research_mysql2"),
}

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGO_DB  = os.getenv("MONGODB_DB",  "research_db")

OUT_DIR = Path(__file__).parent / "neo4j_exportpy"
OUT_DIR.mkdir(exist_ok=True)

# Skip pair generation for papers with more authors than this.
# Consortium papers (100+ authors) each generate thousands of pairs
# that inflate co-authorship counts with little analytical value.
MAX_AUTHORS_PER_PAPER = 50

# ── Name cleaning ─────────────────────────────────────────────────────────────

_STRIP_RE = re.compile(r"^[\s'\"\[\]\\]+|[\s'\"\[\]\\]+$")
_SPACE_RE = re.compile(r"\s+")

def clean_name(name: str) -> str:
    if not name:
        return ""
    return _SPACE_RE.sub(" ", _STRIP_RE.sub("", str(name))).strip()

# ── Streaming MySQL helper ────────────────────────────────────────────────────
# buffered=False tells mysql-connector to NOT load the entire result into RAM.
# Rows are fetched from the server one at a time as the Python loop advances.

def stream_mysql(query: str, desc: str, params=None):
    """
    Yields rows as dicts using an unbuffered server-side cursor.
    Shows a tqdm progress bar counting rows as they arrive.
    Safe for tables with millions of rows.
    """
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    # Use a regular (non-dict) cursor with buffered=False for streaming.
    # We convert to dict manually so column names are available.
    cur = conn.cursor(buffered=False)
    cur.execute(query, params or ())
    col_names = [d[0] for d in cur.description]

    bar = tqdm(cur, desc=desc, unit=" rows", mininterval=0.5, dynamic_ncols=True)
    try:
        for raw_row in bar:
            yield dict(zip(col_names, raw_row))
    finally:
        bar.close()
        cur.close()
        conn.close()

def buffered_mysql(query: str, desc: str, params=None):
    """
    Loads result into memory (only use for small tables like journals/years/sources).
    """
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cur = conn.cursor(dictionary=True, buffered=True)
    print(f"  Querying: {desc}...")
    cur.execute(query, params or ())
    rows = cur.fetchall()
    cur.close()
    conn.close()
    print(f"  Retrieved {len(rows):,} rows")
    return rows

# ── CSV streaming writer ──────────────────────────────────────────────────────

def stream_to_csv(filename: str, headers: list, row_iter, expected: int = None):
    """
    Write rows directly to CSV as they come from row_iter.
    Never holds more than one row in memory at a time.
    """
    path = OUT_DIR / filename
    count = 0
    t0 = time.time()
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        for row in row_iter:
            if isinstance(row, dict):
                writer.writerow([row.get(h, "") for h in headers])
            else:
                writer.writerow(row)
            count += 1
    elapsed = time.time() - t0
    print(f"  ✓ {filename}: {count:,} rows  ({elapsed:.1f}s)")
    return count

def write_list_to_csv(filename: str, headers: list, rows: list):
    path = OUT_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        for row in tqdm(rows, desc=f"  Writing {filename}", unit=" rows",
                        mininterval=0.5, dynamic_ncols=True):
            if isinstance(row, dict):
                writer.writerow([row.get(h, "") for h in headers])
            else:
                writer.writerow(row)
    print(f"  ✓ {filename}: {len(rows):,} rows")

# ── Export functions ──────────────────────────────────────────────────────────

def export_papers():
    print("\n[1/8] Exporting papers...")
    query = """
        SELECT DISTINCT
          p.paper_id,
          p.title,
          p.publish_year  AS year,
          p.doi,
          p.has_full_text,
          p.is_covid19,
          j.journal_name  AS journal,
          s.source_name   AS source
        FROM papers p
        INNER JOIN paper_authors pa ON pa.paper_id  = p.paper_id
        LEFT  JOIN journals      j  ON j.journal_id = p.journal_id
        LEFT  JOIN sources       s  ON s.source_id  = p.source_id
        WHERE p.title IS NOT NULL AND TRIM(p.title) <> ''
        ORDER BY p.paper_id
    """
    headers = ["paper_id","title","year","doi","has_full_text","is_covid19","journal","source"]
    stream_to_csv("papers.csv", headers,
                  stream_mysql(query, "  papers"))


def export_authors():
    print("\n[2/8] Exporting authors...")
    query = """
        SELECT DISTINCT
          a.author_id,
          a.author_name
        FROM authors a
        INNER JOIN paper_authors pa ON pa.author_id = a.author_id
        WHERE a.author_name IS NOT NULL
          AND TRIM(a.author_name) <> ''
          AND LENGTH(TRIM(a.author_name)) >= 2
        ORDER BY a.author_id
    """
    # Clean name during streaming — no intermediate list
    def transform(rows):
        for row in rows:
            name = clean_name(row["author_name"])
            if len(name) >= 2:
                yield {"author_id": row["author_id"], "author_name": name}

    stream_to_csv("authors.csv", ["author_id","author_name"],
                  transform(stream_mysql(query, "  authors")))


def export_paper_authors():
    """
    Exports paper_authors.csv AND computes co_authored.csv in one streaming pass.
    
    By streaming paper_authors ordered by paper_id we can:
      - Write each row to paper_authors.csv immediately (O(1) memory per row)
      - Accumulate author lists per paper in a tiny rolling buffer
      - Generate co-author pairs when the paper changes
      - Accumulate pair counts in a dict
    
    This completely avoids the SQL self-join that caused OOM.
    The SQL self-join on 5.4M rows creates a 5.4M × 5.4M intermediate table
    that blows both MySQL's tmp_table_size and Node/Python's heap.
    """
    print("\n[3/8] Exporting paper_authors + computing co-authorship pairs...")
    print(f"  (Papers with >{MAX_AUTHORS_PER_PAPER} authors skipped for co-authorship)")

    query = """
        SELECT
          pa.paper_id,
          pa.author_order,
          a.author_name
        FROM paper_authors pa
        INNER JOIN authors a ON a.author_id = pa.author_id
        INNER JOIN papers  p ON p.paper_id  = pa.paper_id
        WHERE p.title IS NOT NULL
          AND a.author_name IS NOT NULL
          AND LENGTH(TRIM(a.author_name)) >= 2
        ORDER BY pa.paper_id, pa.author_order
    """

    # co_auth accumulates (author_a, author_b) → shared_paper_count
    # Using a defaultdict(int) — only unique pairs are stored, not all rows.
    co_auth = defaultdict(int)

    pa_path = OUT_DIR / "paper_authors.csv"
    pa_count = 0
    pair_count = 0

    current_paper_id = None
    current_authors  = []   # rolling buffer for current paper, cleared on paper change

    with open(pa_path, "w", newline="", encoding="utf-8") as pa_f:
        pa_writer = csv.writer(pa_f, quoting=csv.QUOTE_ALL)
        pa_writer.writerow(["paper_id", "author_name", "author_order"])

        def flush_paper(paper_id, authors):
            """Generate all pairs from the current paper's author list."""
            nonlocal pair_count
            n = len(authors)
            if n < 2 or n > MAX_AUTHORS_PER_PAPER:
                return
            for i in range(n):
                for j in range(i + 1, n):
                    a, b = authors[i], authors[j]
                    if a == b:
                        continue
                    key = (a, b) if a < b else (b, a)
                    co_auth[key] += 1
                    pair_count += 1

        for row in stream_mysql(query, "  paper_authors"):
            name = clean_name(row["author_name"])
            if len(name) < 2:
                continue

            paper_id = row["paper_id"]

            # Paper boundary — flush the previous paper's authors
            if paper_id != current_paper_id:
                if current_paper_id is not None:
                    flush_paper(current_paper_id, current_authors)
                current_paper_id = paper_id
                current_authors  = []

            current_authors.append(name)

            pa_writer.writerow([paper_id, name, row["author_order"]])
            pa_count += 1

        # Flush the very last paper
        if current_paper_id is not None:
            flush_paper(current_paper_id, current_authors)

    print(f"  ✓ paper_authors.csv: {pa_count:,} rows")
    print(f"  Unique co-author pairs found: {len(co_auth):,}")
    print(f"  Total pair-paper hits: {pair_count:,}")

    # Write co_authored.csv — sort by shared_papers descending
    print("\n[4/8] Writing co_authored.csv...")
    write_list_to_csv(
        "co_authored.csv",
        ["author_a", "author_b", "shared_papers"],
        [{"author_a": a, "author_b": b, "shared_papers": c}
         for (a, b), c in sorted(co_auth.items(), key=lambda x: -x[1])]
    )


def export_journals():
    print("\n[5/8] Exporting journals...")
    # Journals table is small enough to buffer
    rows = buffered_mysql("""
        SELECT DISTINCT
          j.journal_name,
          COALESCE(jr.best_quartile, 'UNRANKED') AS best_quartile,
          jr.sjr_rank,
          jr.sjr_index,
          jr.h_index,
          jr.citescore,
          jr.country,
          jr.oa,
          jr.publisher
        FROM journals j
        INNER JOIN papers p ON p.journal_id = j.journal_id
        LEFT  JOIN journal_rankings jr
              ON LOWER(jr.title) = LOWER(j.journal_name)
        WHERE j.journal_name IS NOT NULL
          AND TRIM(j.journal_name) <> ''
        ORDER BY j.journal_name
    """, "journals")
    write_list_to_csv(
        "journals.csv",
        ["journal_name","best_quartile","sjr_rank","sjr_index",
         "h_index","citescore","country","oa","publisher"],
        rows
    )


def export_years_and_sources():
    print("\n[6/8] Exporting years and sources...")
    years = buffered_mysql("""
        SELECT DISTINCT publish_year AS year
        FROM papers
        WHERE publish_year IS NOT NULL
          AND publish_year BETWEEN 1900 AND 2030
        ORDER BY publish_year
    """, "years")
    write_list_to_csv("years.csv", ["year"], years)

    sources = buffered_mysql("""
        SELECT DISTINCT s.source_name
        FROM sources s
        INNER JOIN papers p ON p.source_id = s.source_id
        WHERE s.source_name IS NOT NULL
        ORDER BY s.source_name
    """, "sources")
    write_list_to_csv("sources.csv", ["source_name"], sources)


def export_topics():
    """
    Read keywords from MongoDB.
    Uses allowDiskUse=True so Mongo can spill to disk on large aggregations
    instead of hitting its 100MB memory limit and failing.
    """
    print("\n[7/8] Exporting topics from MongoDB...")
    client = MongoClient(MONGO_URI)
    col    = client[MONGO_DB]["papers"]

    print("  Aggregating distinct topics (allowDiskUse=True)...")
    topics = list(col.aggregate([
        {"$unwind": "$keywords"},
        {"$match":  {"keywords": {"$nin": [None, "", "nan"]}}},
        {"$group":  {"_id": {"$toLower": "$keywords"}, "c": {"$sum": 1}}},
        {"$match":  {"c": {"$gte": 3}}},
        {"$project":{"topic": "$_id", "_id": 0}},
        {"$sort":   {"topic": 1}},
    ], allowDiskUse=True))
    write_list_to_csv("topics.csv", ["topic"], topics)

    print("\n[8/8] Exporting paper → topic links...")
    paper_topics = list(col.aggregate([
        {"$unwind": "$keywords"},
        {"$match":  {"keywords": {"$nin": [None, "", "nan"]}}},
        {"$group":  {"_id": {"paper_id": "$paper_id",
                             "topic": {"$toLower": "$keywords"}}}},
        {"$project":{"paper_id": "$_id.paper_id",
                     "topic":    "$_id.topic",
                     "_id":      0}},
        {"$sort":   {"paper_id": 1, "topic": 1}},
    ], allowDiskUse=True))
    write_list_to_csv("paper_topics.csv", ["paper_id","topic"], paper_topics)

    client.close()


# ── Summary printer ───────────────────────────────────────────────────────────

def count_csv_rows(filename):
    path = OUT_DIR / filename
    if not path.exists():
        return 0
    with open(path, encoding="utf-8") as f:
        return sum(1 for _ in f) - 1  # subtract header

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    total_start = time.time()

    print("=" * 60)
    print("  Neo4j CSV Export — streaming, memory-safe")
    print(f"  Output: {OUT_DIR}")
    print("=" * 60)

    export_papers()
    export_authors()
    export_paper_authors()    # also writes co_authored.csv
    export_journals()
    export_years_and_sources()
    export_topics()

    elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print("  Export complete")
    print(f"  Total time: {elapsed/60:.1f} minutes")
    print("=" * 60)

    print("\n📊 Final counts:")
    for f in ["papers.csv","authors.csv","paper_authors.csv",
              "co_authored.csv","journals.csv","years.csv",
              "sources.csv","topics.csv","paper_topics.csv"]:
        n = count_csv_rows(f)
        print(f"  {f:<22} {n:>12,} rows")

    print("\nNext steps:")
    print("  1. Copy all CSVs from neo4j_export/ to Neo4j import directory")
    print("  2. Run the Cypher import script in Neo4j Browser")