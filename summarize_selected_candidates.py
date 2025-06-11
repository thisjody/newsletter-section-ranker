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

# Configuration from .env or defaults
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
OUTPUT_PATH = Path(os.getenv("SUMMARY_OUTPUT_PATH", "summaries/summarized_candidates.json"))
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

# Vertex or Public Gemini initialization
if USE_VERTEX:
    from vertexai import init as vertexai_init
    from vertexai.generative_models import GenerativeModel, Part
    vertexai_init(project=PROJECT_ID, location=LOCATION, credentials=CREDENTIALS_PATH)
else:
    from google.generativeai import configure as genai_configure
    from google.generativeai import GenerativeModel
    genai_configure(api_key=GOOGLE_API_KEY)

def read_ids_from_dir(directory: Path) -> list[str]:
    all_ids = []
    for path in sorted(directory.glob("*.json")):
        try:
            with open(path) as f:
                ids = json.load(f)
                if isinstance(ids, list):
                    all_ids.extend(ids)
        except Exception as e:
            print(f"⚠️ Error reading {path}: {e}")
    return all_ids

def fetch_candidate_content(article_id):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        row = con.execute("""
            SELECT id, url, content
            FROM link_embeddings
            WHERE id = ?
        """, [article_id]).fetchone()
    finally:
        con.close()
    return row if row else None

def generate_summary(text):
    try:
        model = GenerativeModel(SUMMARY_MODEL)
        prompt = f"Summarize this in {CHAR_LIMIT} characters or less:\n{text}"

        if USE_VERTEX:
            response = model.generate_content([Part.from_text(prompt)])
        else:
            response = model.generate_content(prompt)

        return response.text.strip()
    except (PermissionDenied, NotFound) as e:
        return f"[ERROR: {e.message}]"
    except Exception as e:
        return f"[ERROR: Unexpected failure: {e}]"

def main():
    selected_ids = read_ids_from_dir(SINGLE_DIR) + read_ids_from_dir(CLUSTERED_DIR)
    print(f"🔍 Processing {len(selected_ids)} selected candidates...")

    results = []
    for aid in tqdm(selected_ids, desc="Summarizing", unit="article"):
        row = fetch_candidate_content(aid)
        if not row:
            continue
        article_id, url, content = row
        summary = generate_summary(content)
        results.append({
            "id": article_id,
            "url": url,
            "summary": summary
        })

    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    print(f"✅ Wrote {len(results)} summaries → {OUTPUT_PATH}")

if __name__ == "__main__":
    main()

