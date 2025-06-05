#!/usr/bin/env python
"""
Annotate candidate articles with best-matching sections based on cosine distance,
and optionally dump per-section match results as JSON files.
Supports idempotent runs by overwriting prior outputs.
"""

import duckdb
import argparse
import json
import os
import dotenv
from pathlib import Path
from collections import defaultdict

dotenv.load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/newsletter_embeddings.duckdb")
MATCH_TABLE = os.getenv("MATCH_TABLE", "candidate_section_matches")
DEFAULT_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", 0.40))
DEFAULT_TOP_K = int(os.getenv("TOP_K_MATCHES", 3))
DEFAULT_OUTPUT_DIR = os.getenv("SECTION_JSON_OUTPUT_DIR", "section_matches")
SUMMARY_CHAR_LIMIT = int(os.getenv("SUMMARY_CHAR_LIMIT", 280))

def annotate_candidates(similarity_threshold=DEFAULT_THRESHOLD, top_k=DEFAULT_TOP_K, ranked_output=True, dump_json=None, per_section_json_dir=None):
    con = duckdb.connect(DUCKDB_PATH)
    con.execute("LOAD 'vss'")

    query = f"""
        WITH ranked_matches AS (
            SELECT
                c.id AS candidate_id,
                s.section AS matched_section,
                list_cosine_distance(c.embedding, s.embedding) AS cosine_distance,
                ROW_NUMBER() OVER (
                    PARTITION BY c.id
                    ORDER BY list_cosine_distance(c.embedding, s.embedding)
                ) AS rnk
            FROM link_embeddings c, section_fingerprints s
            WHERE c.section = 'CANDIDATE'
              AND s.section != 'CANDIDATE'
              AND list_cosine_distance(c.embedding, s.embedding) <= {similarity_threshold}
        )
        SELECT candidate_id, matched_section, cosine_distance
        FROM ranked_matches
        WHERE rnk <= {top_k}
        ORDER BY candidate_id, rnk
    """

    results = con.execute(query).fetchall()

    if per_section_json_dir:
        output_dir = Path(per_section_json_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Clear old JSONs to ensure idempotence
        for old_file in output_dir.glob("*.json"):
            old_file.unlink()

        matches_by_section = defaultdict(list)

        for row in results:
            candidate_id, section, distance = row
            row_meta = con.execute(
                "SELECT url, filename, content FROM link_embeddings WHERE id = ?",
                [candidate_id]
            ).fetchone()

            url, filename, content = row_meta
            summary = (content or "").strip().replace("\n", " ")[:SUMMARY_CHAR_LIMIT] + "…" if content else ""

            matches_by_section[section].append({
                "candidate_id": candidate_id,
                "url": url,
                "filename": filename,
                "cosine_distance": round(distance, 4),
                "summary": summary
            })

        for section, matches in matches_by_section.items():
            sorted_matches = sorted(matches, key=lambda m: m["cosine_distance"])
            output_path = output_dir / f"{section.lower()}.json"
            with open(output_path, "w") as f:
                json.dump(sorted_matches, f, indent=2)
            print(f"📝 Wrote {len(sorted_matches)} matches to {output_path}")
        return

    # Fallback to print output if not dumping
    print(f"🔍 Showing top {top_k} matches per candidate (threshold={similarity_threshold}):")
    for r in results:
        print(r)
    print(f"✅ Returned {len(results)} matches")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--similarity_threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--top_k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--per_section_json_dir", type=str, default=None)

    args = parser.parse_args()

    annotate_candidates(
        similarity_threshold=args.similarity_threshold,
        top_k=args.top_k,
        per_section_json_dir=args.per_section_json_dir
    )

