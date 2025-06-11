#!/usr/bin/env python3

import os
import json
import duckdb
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from google.api_core.exceptions import PermissionDenied, NotFound

# Load environment variables
load_dotenv()

# Configuration
USE_VERTEX = os.getenv("GOOGLE_GENAI_USE_VERTEXAI", "FALSE").upper() == "TRUE"
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION", "us-central1")
CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
SUMMARY_MODEL = os.getenv("SUMMARY_MODEL", "gemini-2.0-flash")
CHAR_LIMIT = int(os.getenv("SUMMARY_CHAR_LIMIT", "280"))

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/newsletter_embeddings.duckdb")
CANDIDATE_TABLE = os.getenv("MATCH_TABLE", "candidate_section_matches")

SINGLE_DIR = Path(os.getenv("SELECTED_IDS_SINGLE_DIR", "selected_ids/single"))
CLUSTERED_DIR = Path(os.getenv("SELECTED_IDS_CLUSTERED_DIR", "selected_ids/clustered"))
OUT_SINGLE = Path("summaries/summarized_candidates.json")
OUT_CLUSTER = Path("summaries/summarized_candidates_cluster.json")

for p in [OUT_SINGLE.parent, OUT_CLUSTER.parent]:
    p.mkdir(parents=True, exist_ok=True)

# Vertex or Public Gemini initialization
if USE_VERTEX:
    from vertexai import init as vertexai_init
    from vertexai.generative_models import GenerativeModel, Part
    vertexai_init(project=PROJECT_ID, location=LOCATION, credentials=CREDENTIALS_PATH)
else:
    from google.generativeai import configure as genai_configure
    from google.generativeai import GenerativeModel
    genai_configure(api_key=GOOGLE_API_KEY)

def fetch_candidate_content(article_id):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        row = con.execute("SELECT id, url, content FROM link_embeddings WHERE id = ?", [article_id]).fetchone()
    finally:
        con.close()
    return row if row else None

def generate_summary(text):
    try:
        model = GenerativeModel(SUMMARY_MODEL)
        prompt = f"Summarize this in {CHAR_LIMIT} characters or less:\n{text}"
        response = model.generate_content([Part.from_text(prompt)] if USE_VERTEX else prompt)
        return response.text.strip()
    except (PermissionDenied, NotFound) as e:
        return f"[ERROR: {e.message}]"
    except Exception as e:
        return f"[ERROR: Unexpected failure: {e}]"

def summarize_batch(directory: Path) -> list[dict]:
    summaries = []
    for path in sorted(directory.glob("*.json")):
        section = path.stem.upper()
        try:
            with open(path) as f:
                ids = json.load(f)
        except Exception as e:
            print(f"⚠️ Error reading {path}: {e}")
            continue

        for aid in tqdm(ids, desc=f"Summarizing {section}", unit="article"):
            row = fetch_candidate_content(aid)
            if not row:
                continue
            article_id, url, content = row
            summary = generate_summary(content)
            summaries.append({
                "id": article_id,
                "url": url,
                "section": section,
                "summary": summary
            })
    return summaries

def main():
    print("🔁 Starting summarization for SINGLE...")
    single_summaries = summarize_batch(SINGLE_DIR)
    with open(OUT_SINGLE, "w") as f:
        json.dump(single_summaries, f, indent=2)
    print(f"✅ Wrote {len(single_summaries)} single summaries → {OUT_SINGLE}")

    print("🔁 Starting summarization for CLUSTER...")
    cluster_summaries = summarize_batch(CLUSTERED_DIR)
    with open(OUT_CLUSTER, "w") as f:
        json.dump(cluster_summaries, f, indent=2)
    print(f"✅ Wrote {len(cluster_summaries)} cluster summaries → {OUT_CLUSTER}")

if __name__ == "__main__":
    main()

