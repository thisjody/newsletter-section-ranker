#!/usr/bin/env python
"""
Annotate candidate articles by matching their embeddings against section cluster fingerprints,
optionally dumping per-section JSON files, and always storing results in DuckDB.
"""

import os
import json
import duckdb
import dotenv
import numpy as np
from tqdm import tqdm
from pathlib import Path
from collections import defaultdict
import argparse

# Argument parsing
parser = argparse.ArgumentParser()
parser.add_argument(
    "--per_section_json_dir",
    type=str,
    help="Directory to dump per-section JSONs (optional). Overrides CLUSTER_MATCH_OUTPUT_DIR"
)
args = parser.parse_args()

# Load env
dotenv.load_dotenv()
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/newsletter_embeddings.duckdb")
CANDIDATE_TABLE = os.getenv("CANDIDATE_TABLE", "link_embeddings")
CLUSTER_TABLE = os.getenv("CLUSTER_FINGERPRINT_TABLE", "section_cluster_fingerprints")
DEFAULT_OUTPUT_DIR = os.getenv("CLUSTER_MATCH_OUTPUT_DIR", "section_cluster_matches")
SUMMARY_CHAR_LIMIT = int(os.getenv("SUMMARY_CHAR_LIMIT", 280))
TOP_K = int(os.getenv("TOP_K_CLUSTER_MATCHES", 5))

# Determine output directory if JSON dumping is requested
OUTPUT_DIR = args.per_section_json_dir or DEFAULT_OUTPUT_DIR
dump_json = bool(args.per_section_json_dir or os.getenv("CLUSTER_MATCH_OUTPUT_DIR"))

# Connect to DuckDB
con = duckdb.connect(DUCKDB_PATH, config={"enable_external_access": True})
con.execute("LOAD 'vss'")

if dump_json:
    print(f"📡 Matching candidates to clustered fingerprints (k={TOP_K}) from `{CLUSTER_TABLE}`")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for file in Path(OUTPUT_DIR).glob("*.json"):
        file.unlink()

# Load candidate embeddings
candidates = con.execute(f"""
    SELECT id, url, filename, content, embedding
    FROM {CANDIDATE_TABLE}
    WHERE section = 'CANDIDATE'
""").fetchall()

# Load clustered section fingerprints
clusters = con.execute(f"""
    SELECT section, cluster_id, embedding
    FROM {CLUSTER_TABLE}
""").fetchall()

# Match candidates to cluster fingerprints
matches_by_section = defaultdict(list)

for cand_id, url, filename, content, cand_emb in tqdm(candidates, desc="📊 Processing matches"):
    if cand_emb is None:
        continue
    cand_vec = np.array(cand_emb)
    best_match = None

    for section, cluster_id, cluster_emb in clusters:
        cluster_vec = np.array(cluster_emb)
        cosine_dist = 1 - np.dot(cand_vec, cluster_vec) / (np.linalg.norm(cand_vec) * np.linalg.norm(cluster_vec) + 1e-10)

        if len(matches_by_section[section]) < TOP_K or cosine_dist < max(m["cosine_distance"] for m in matches_by_section[section]):
            summary = (content or "").strip().replace("\n", " ")[:SUMMARY_CHAR_LIMIT] + "…" if content else ""
            matches_by_section[section].append({
                "candidate_id": cand_id,
                "url": url,
                "filename": filename,
                "cluster_id": cluster_id,
                "cosine_distance": round(cosine_dist, 4),
                "summary": summary
            })

# Truncate to top-k per section
for section in matches_by_section:
    matches_by_section[section] = sorted(matches_by_section[section], key=lambda m: m["cosine_distance"])[:TOP_K]

# Dump JSONs if requested
if dump_json:
    for section, matches in matches_by_section.items():
        out_path = os.path.join(OUTPUT_DIR, f"{section.lower()}.json")
        with open(out_path, "w") as f:
            json.dump(matches, f, indent=2)
        print(f"📝 Wrote {len(matches)} clustered matches to {out_path}")

# Save results to DuckDB
print(f"💾 Saving clustered matches to table: candidate_cluster_section_matches")
con.execute("DROP TABLE IF EXISTS candidate_cluster_section_matches")
con.execute("""
    CREATE TABLE candidate_cluster_section_matches (
        candidate_id TEXT,
        section TEXT,
        cluster_id INTEGER,
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
        m["cluster_id"],
        m["cosine_distance"],
        m["url"],
        m["filename"],
        m["summary"]
    )
    for section, matches in matches_by_section.items()
    for m in matches
]

con.executemany("""
    INSERT INTO candidate_cluster_section_matches (
        candidate_id, section, cluster_id, cosine_distance, url, filename, summary
    ) VALUES (?, ?, ?, ?, ?, ?, ?)
""", insert_rows)
print(f"✅ Inserted {len(insert_rows)} rows into candidate_cluster_section_matches")

