#!/usr/bin/env python
"""
Annotate candidate articles with best-matching sections based on single centroid distance,
dumping per-section match results as JSON files and saving results to DuckDB.
"""

import duckdb
import argparse
import json
import os
import dotenv
from pathlib import Path
from collections import defaultdict
from tqdm import tqdm  # ✅ Added for progress bar

# Load environment variables
dotenv.load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/newsletter_embeddings.duckdb")
MATCH_TABLE = os.getenv("MATCH_TABLE", "candidate_section_matches")
CANDIDATE_TABLE = os.getenv("CANDIDATE_TABLE", "link_embeddings")
FINGERPRINT_TABLE = os.getenv("FINGERPRINT_TABLE", "section_fingerprints")
DEFAULT_OUTPUT_DIR = os.getenv("SECTION_JSON_OUTPUT_DIR", "section_matches")
SUMMARY_CHAR_LIMIT = int(os.getenv("SUMMARY_CHAR_LIMIT", 280))
TOP_K = int(os.getenv("TOP_K_MATCHES", 5))
SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", 0.35))

# Parse command-line arguments
parser = argparse.ArgumentParser()
parser.add_argument("--per_section_json_dir", type=str, default=None)
args = parser.parse_args()

# Set output directory
OUTPUT_DIR = args.per_section_json_dir or DEFAULT_OUTPUT_DIR
os.makedirs(OUTPUT_DIR, exist_ok=True)
for f in Path(OUTPUT_DIR).glob("*.json"):
    f.unlink()

# Connect to DuckDB
con = duckdb.connect(DUCKDB_PATH, config={"enable_external_access": True})
con.execute("LOAD 'vss'")

# Load candidates and section fingerprints
candidates = con.execute(f"""
    SELECT id, url, filename, content, embedding
    FROM {CANDIDATE_TABLE}
    WHERE section = 'CANDIDATE'
""").fetchall()

sections = con.execute(f"""
    SELECT section, embedding
    FROM {FINGERPRINT_TABLE}
    WHERE section != 'CANDIDATE'
""").fetchall()

# Match candidates to sections
matches_by_section = defaultdict(list)

for cand_id, url, filename, content, cand_emb in tqdm(candidates, desc="📊 Processing matches"):
    if cand_emb is None:
        continue
    for section, sec_emb in sections:
        dist = con.execute("SELECT list_cosine_distance(?, ?)", [cand_emb, sec_emb]).fetchone()[0]
        if dist <= SIMILARITY_THRESHOLD:
            summary = (content or "").strip().replace("\n", " ")[:SUMMARY_CHAR_LIMIT] + "…" if content else ""
            matches_by_section[section].append({
                "candidate_id": cand_id,
                "url": url,
                "filename": filename,
                "cosine_distance": round(dist, 4),
                "summary": summary
            })

# Write JSON files
for section, matches in matches_by_section.items():
    sorted_matches = sorted(matches, key=lambda m: m["cosine_distance"])[:TOP_K]
    with open(Path(OUTPUT_DIR) / f"{section.lower()}.json", "w") as f:
        json.dump(sorted_matches, f, indent=2)
    print(f"📝 Wrote {len(sorted_matches)} matches to {section.lower()}.json")

# Save results to DuckDB
print(f"💾 Saving single centroid matches to table: {MATCH_TABLE}")
con.execute(f"DROP TABLE IF EXISTS {MATCH_TABLE}")
con.execute(f"""
    CREATE TABLE {MATCH_TABLE} (
        candidate_id TEXT,
        section TEXT,
        cosine_distance DOUBLE,
        url TEXT,
        filename TEXT,
        summary TEXT
    )
""")

insert_rows = [
    (
        m["candidate_id"],
        section,
        m["cosine_distance"],
        m["url"],
        m["filename"],
        m["summary"]
    )
    for section, matches in matches_by_section.items()
    for m in matches
]

con.executemany(f"""
    INSERT INTO {MATCH_TABLE} (
        candidate_id, section, cosine_distance, url, filename, summary
    ) VALUES (?, ?, ?, ?, ?, ?)
""", insert_rows)

print(f"✅ Inserted {len(insert_rows)} rows into {MATCH_TABLE}")
