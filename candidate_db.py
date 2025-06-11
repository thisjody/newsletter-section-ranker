#!/usr/bin/env python3

import os
import json
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Match directories from environment
SINGLE_DIR = os.getenv("SECTION_JSON_OUTPUT_DIR", "section_matches")
CLUSTER_DIR = os.getenv("CLUSTER_JSON_OUTPUT_DIR", "section_cluster_matches")
SUMMARY_SINGLE = Path("summaries/summarized_candidates.json")
SUMMARY_CLUSTER = Path("summaries/summarized_candidates_cluster.json")

# Streamlit page setup
st.set_page_config(page_title="Newsletter Section Matches", layout="wide")
st.title("📚 Newsletter Section Review Tool")

# Tabbed interface
tab1, tab2 = st.tabs(["🧠 Section Matches", "📝 Summarized Candidates"])

with tab1:
    # Toggle between match modes
    mode = st.radio("📊 Match Mode", ["Single Centroid", "Clustered"], horizontal=True)
    MATCH_DIR = SINGLE_DIR if mode == "Single Centroid" else CLUSTER_DIR
    MODE_DIR = "single" if mode == "Single Centroid" else "clustered"

    # Gather available match files
    section_files = sorted(Path(MATCH_DIR).glob("*.json"))
    if not section_files:
        st.warning(f"No match files found in `{MATCH_DIR}`")
        st.stop()

    # Section selection
    section = st.selectbox("🗂️ Select Section", [f.stem.upper() for f in section_files])
    file_path = next(f for f in section_files if f.stem.upper() == section)

    with open(file_path) as f:
        matches = json.load(f)

    st.subheader(f"Top {len(matches)} {mode.lower()} matches for: `{section}`")

    selected_ids = []

    for m in matches:
        st.markdown("---")
        if mode == "Clustered":
            st.markdown(f"**Distance**: {m.get('cosine_distance', '?')} | **Cluster**: {m.get('cluster_id', '?')}")
        else:
            st.markdown(f"**Distance**: {m.get('cosine_distance', '?')}")

        st.markdown(f"[🔗 {m.get('url', 'URL missing')}]({m.get('url', '')})")
        st.code(m.get("summary", "No summary available"), language="html")

        candidate_id = m.get("id")
        if candidate_id:
            if st.checkbox("✅ Select this article", key=f"{MODE_DIR}_{section}_{candidate_id}"):
                selected_ids.append(candidate_id)

    # Handle export
    if st.button("📤 Export selected IDs for this section"):
        export_path = Path("selected_ids") / MODE_DIR / f"{section}.json"
        export_path.parent.mkdir(parents=True, exist_ok=True)
        with open(export_path, "w") as f:
            json.dump(selected_ids, f, indent=2)
        st.success(f"Exported {len(selected_ids)} IDs → `{export_path}`")

with tab2:
    st.subheader("📝 Summarized Candidate Articles")

    # Choose which summary file to display
    summary_mode = st.radio("🧮 View Summaries From", ["Single Centroid", "Clustered"], horizontal=True)
    summary_path = SUMMARY_SINGLE if summary_mode == "Single Centroid" else SUMMARY_CLUSTER

    if not summary_path.exists():
        st.info(f"No summaries found yet for `{summary_mode}` mode. Run summarization first.")
    else:
        try:
            with open(summary_path) as f:
                summaries = json.load(f)

            for entry in summaries:
                st.markdown("---")
                st.markdown(f"[🔗 {entry.get('url', 'URL missing')}]({entry.get('url', '')})")
                st.markdown(f"**📚 Section**: `{entry.get('section', '?')}`")
                st.markdown(entry.get("summary", "No summary available"))
        except Exception as e:
            st.error(f"Failed to load summaries: {e}")

