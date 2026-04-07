"""Load Kaggle journal ranking CSV into MySQL table `journal_rankings`.

Usage:
  python scripts/load_journal_rankings.py --csv /path/to/journal_ranking_data.csv
  python scripts/load_journal_rankings.py /path/to/journal_ranking_data.csv
  python scripts/load_journal_rankings.py   # auto-detects common file names
"""
import argparse
import csv
import json
import math
import os
from typing import Any



def is_missing(value: Any) -> bool:
    if value is None:
        return True
    text = str(value).strip()
    return text == '' or text.lower() in {'nan', 'none', 'null', 'na'}


def to_bool(value: Any):
    if is_missing(value):
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return 1
    if text in {"false", "0", "no"}:
        return 0
    return None


def to_num(value: Any, as_int=False):
    if is_missing(value):
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(num):
        return None
    return int(num) if as_int else num


def to_json_list(value: Any):
    if is_missing(value):
        return None
    text = str(value).strip()
    try:
        if text.startswith("[") and text.endswith("]"):
            parsed = json.loads(text.replace("'", '"'))
            return json.dumps(parsed)
    except Exception:
        pass
    return json.dumps([item.strip() for item in text.split(",") if item.strip()])


def resolve_csv_path(cli_csv: str | None):
    if cli_csv:
        return cli_csv

    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.join(os.getcwd(), "journal_ranking_data.csv"),
        os.path.join(os.getcwd(), "journal_ranking_dataset.csv"),
        os.path.join(script_dir, "journal_ranking_data.csv"),
        os.path.join(script_dir, "..", "journal_ranking_data.csv"),
    ]

    for candidate in candidates:
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

    raise FileNotFoundError(
        "CSV file not found. Pass --csv <path> or place journal_ranking_data.csv in project root/scripts."
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", nargs="?", help="D://Amrita//Research//DBMS//DBMS PROJECT//journal_ranking_data.csv")
    parser.add_argument("--csv", dest="csv_flag", help="D://Amrita//Research//DBMS//DBMS PROJECT//journal_ranking_data.csv")
    parser.add_argument("--chunk-size", type=int, default=2000)
    args = parser.parse_args()

    csv_path = resolve_csv_path(args.csv_flag or args.csv)

    try:
        import mysql.connector
    except ModuleNotFoundError as exc:
        raise ModuleNotFoundError("mysql-connector-python is required. Install with: pip install mysql-connector-python") from exc

    mysql_config = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", "root"),
        "database": os.getenv("MYSQL_DATABASE", "research_mysql2"),
    }

    conn = mysql.connector.connect(**mysql_config)
    cur = conn.cursor()

    insert_sql = """
      INSERT INTO journal_rankings (
        sjr_rank, title, oa, country, sjr_index, citescore, h_index,
        best_quartile, best_categories, best_subject_area, best_subject_rank,
        total_docs, total_docs_3y, total_refs, total_cites_3y, citable_docs_3y,
        cites_per_doc_2y, refs_per_doc, publisher, core_collection, coverage,
        active, in_press, iso_language_code, asjc_codes
      ) VALUES (
        %(sjr_rank)s, %(title)s, %(oa)s, %(country)s, %(sjr_index)s, %(citescore)s, %(h_index)s,
        %(best_quartile)s, %(best_categories)s, %(best_subject_area)s, %(best_subject_rank)s,
        %(total_docs)s, %(total_docs_3y)s, %(total_refs)s, %(total_cites_3y)s, %(citable_docs_3y)s,
        %(cites_per_doc_2y)s, %(refs_per_doc)s, %(publisher)s, %(core_collection)s, %(coverage)s,
        %(active)s, %(in_press)s, %(iso_language_code)s, %(asjc_codes)s
      )
      ON DUPLICATE KEY UPDATE
        sjr_rank = VALUES(sjr_rank),
        oa = VALUES(oa),
        country = VALUES(country),
        sjr_index = VALUES(sjr_index),
        citescore = VALUES(citescore),
        h_index = VALUES(h_index),
        best_quartile = VALUES(best_quartile),
        best_categories = VALUES(best_categories),
        best_subject_area = VALUES(best_subject_area),
        best_subject_rank = VALUES(best_subject_rank),
        total_docs = VALUES(total_docs),
        total_docs_3y = VALUES(total_docs_3y),
        total_refs = VALUES(total_refs),
        total_cites_3y = VALUES(total_cites_3y),
        citable_docs_3y = VALUES(citable_docs_3y),
        cites_per_doc_2y = VALUES(cites_per_doc_2y),
        refs_per_doc = VALUES(refs_per_doc),
        publisher = VALUES(publisher),
        core_collection = VALUES(core_collection),
        coverage = VALUES(coverage),
        active = VALUES(active),
        in_press = VALUES(in_press),
        iso_language_code = VALUES(iso_language_code),
        asjc_codes = VALUES(asjc_codes),
        source_updated_at = CURRENT_TIMESTAMP
    """

    mappings = {
        "sjr_rank": "Rank",
        "title": "Title",
        "oa": "OA",
        "country": "Country",
        "sjr_index": "SJR-index",
        "citescore": "CiteScore",
        "h_index": "H-index",
        "best_quartile": "Best Quartile",
        "best_categories": "Best Categories",
        "best_subject_area": "Best Subject Area",
        "best_subject_rank": "Best Subject Rank",
        "total_docs": "Total Docs.",
        "total_docs_3y": "Total Docs. 3y",
        "total_refs": "Total Refs.",
        "total_cites_3y": "Total Cites 3y",
        "citable_docs_3y": "Citable Docs. 3y",
        "cites_per_doc_2y": "Cites/Doc. 2y",
        "refs_per_doc": "Refs./Doc.",
        "publisher": "Publisher",
        "core_collection": "Core Collection",
        "coverage": "Coverage",
        "active": "Active",
        "in_press": "In-Press",
        "iso_language_code": "ISO Language Code",
        "asjc_codes": "ASJC Codes",
    }

    with open(csv_path, mode='r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        rows = []
        total = 0
        for src in reader:
            rows.append({
                "sjr_rank": to_num(src.get(mappings["sjr_rank"]), as_int=True),
                "title": str(src.get(mappings["title"], "")).strip(),
                "oa": to_bool(src.get(mappings["oa"])),
                "country": None if is_missing(src.get(mappings["country"])) else str(src.get(mappings["country"])).strip(),
                "sjr_index": to_num(src.get(mappings["sjr_index"])),
                "citescore": to_num(src.get(mappings["citescore"])),
                "h_index": to_num(src.get(mappings["h_index"]), as_int=True),
                "best_quartile": None if is_missing(src.get(mappings["best_quartile"])) else str(src.get(mappings["best_quartile"])).strip(),
                "best_categories": to_json_list(src.get(mappings["best_categories"])),
                "best_subject_area": None if is_missing(src.get(mappings["best_subject_area"])) else str(src.get(mappings["best_subject_area"])).strip(),
                "best_subject_rank": None if is_missing(src.get(mappings["best_subject_rank"])) else str(src.get(mappings["best_subject_rank"])).strip(),
                "total_docs": to_num(src.get(mappings["total_docs"]), as_int=True),
                "total_docs_3y": to_num(src.get(mappings["total_docs_3y"]), as_int=True),
                "total_refs": to_num(src.get(mappings["total_refs"]), as_int=True),
                "total_cites_3y": to_num(src.get(mappings["total_cites_3y"]), as_int=True),
                "citable_docs_3y": to_num(src.get(mappings["citable_docs_3y"]), as_int=True),
                "cites_per_doc_2y": to_num(src.get(mappings["cites_per_doc_2y"])),
                "refs_per_doc": to_num(src.get(mappings["refs_per_doc"])),
                "publisher": None if is_missing(src.get(mappings["publisher"])) else str(src.get(mappings["publisher"])).strip(),
                "core_collection": None if is_missing(src.get(mappings["core_collection"])) else str(src.get(mappings["core_collection"])).strip(),
                "coverage": None if is_missing(src.get(mappings["coverage"])) else str(src.get(mappings["coverage"])).strip(),
                "active": to_bool(src.get(mappings["active"])),
                "in_press": to_bool(src.get(mappings["in_press"])),
                "iso_language_code": None if is_missing(src.get(mappings["iso_language_code"])) else str(src.get(mappings["iso_language_code"])).strip(),
                "asjc_codes": None if is_missing(src.get(mappings["asjc_codes"])) else str(src.get(mappings["asjc_codes"])).strip(),
            })

            if len(rows) >= args.chunk_size:
                cur.executemany(insert_sql, rows)
                conn.commit()
                total += len(rows)
                print(f"Loaded rows: {total}")
                rows = []

        if rows:
            cur.executemany(insert_sql, rows)
            conn.commit()
            total += len(rows)
            print(f"Loaded rows: {total}")

    cur.close()
    conn.close()
    print(f"Done: journal_rankings updated from {csv_path}.")


if __name__ == "__main__":
    main()
