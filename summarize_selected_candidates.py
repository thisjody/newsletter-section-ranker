#!/usr/bin/env python3

import os
import json
import duckdb
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv
from google.api_core.exceptions import PermissionDenied, NotFound, ServiceUnavailable
import google.generativeai as genai

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
SINGLE_DIR = Path(os.getenv("SELECTED_IDS_SINGLE_DIR", "selected_ids/single"))
CLUSTERED_DIR = Path(os.getenv("SELECTED_IDS_CLUSTERED_DIR", "selected_ids/clustered"))

OUT_SINGLE = Path("summaries/summarized_candidates.json")
OUT_CLUSTER = Path("summaries/summarized_candidates_cluster.json")
for p in [OUT_SINGLE.parent, OUT_CLUSTER.parent]:
    p.mkdir(parents=True, exist_ok=True)

# Initialize LLM model
llm_model = None
if USE_VERTEX:
    if not PROJECT_ID or not LOCATION:
        raise ValueError("Vertex mode requires GOOGLE_CLOUD_PROJECT and GOOGLE_CLOUD_LOCATION to be set.")
    llm_model = genai.GenerativeModel(model_name=SUMMARY_MODEL)
    print(f"LLM initialized via Vertex AI: {SUMMARY_MODEL} @ {PROJECT_ID}/{LOCATION}")
else:
    if not GOOGLE_API_KEY:
        raise ValueError("Public mode requires GOOGLE_API_KEY to be set.")
    genai.configure(api_key=GOOGLE_API_KEY)
    llm_model = genai.GenerativeModel(model_name=SUMMARY_MODEL)
    print(f"LLM initialized via Public API: {SUMMARY_MODEL}")

def fetch_candidate_content(article_id):
    con = duckdb.connect(DUCKDB_PATH)
    try:
        row = con.execute("SELECT id, url, content FROM link_embeddings WHERE id = ?", [article_id]).fetchone()
    finally:
        con.close()
    return row if row else None

def generate_summary(text):
    global llm_model
    prompt = f"Summarize this in {CHAR_LIMIT} characters or less:\n{text}"
    try:
        response = llm_model.generate_content(prompt)
        return response.text.strip()
    except (PermissionDenied, NotFound, ServiceUnavailable) as e:
        print(f"[API ERROR: {type(e).__name__}] {e.message}")
        return f"[ERROR: {e.message}]"
    except Exception as e:
        print(f"[ERROR: Unexpected failure] {e}")
        return f"[ERROR: Unexpected failure: {e}]"

def summarize_batch(directory: Path, mode: str) -> list[dict]:
    summaries = []
    for path in sorted(directory.glob("*.json")):
        section = path.stem.upper()
        try:
            with open(path) as f:
                ids = json.load(f)
        except Exception as e:
            print(f"⚠️ Error reading {path}: {e}")
            continue

        for aid in tqdm(ids, desc=f"{mode.upper()} → {section}", unit="article"):
            row = fetch_candidate_content(aid)
            if not row:
                continue
            article_id, url, content = row
            summary = generate_summary(content)
            summaries.append({
                "id": article_id,
                "url": url,
                "section": section,
                "mode": mode,
                "summary": summary
            })
    return summaries

def main():
    print("🔁 Summarizing SINGLE selections...")
    single = summarize_batch(SINGLE_DIR, mode="single")
    with open(OUT_SINGLE, "w") as f:
        json.dump(single, f, indent=2)
    print(f"✅ Saved {len(single)} summaries to {OUT_SINGLE}")

    print("🔁 Summarizing CLUSTERED selections...")
    cluster = summarize_batch(CLUSTERED_DIR, mode="clustered")
    with open(OUT_CLUSTER, "w") as f:
        json.dump(cluster, f, indent=2)
    print(f"✅ Saved {len(cluster)} summaries to {OUT_CLUSTER}")

if __name__ == "__main__":
    main()

