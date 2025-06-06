#!/usr/bin/env python
# cluster_dashboard.py

import os
import json
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Match directories configured via environment
SINGLE_DIR = os.getenv("SECTION_JSON_OUTPUT_DIR", "section_matches")
CLUSTER_DIR = os.getenv("CLUSTER_JSON_OUTPUT_DIR", "section_cluster_matches")

# Streamlit page setup
st.set_page_config(page_title="Newsletter Section Matches", layout="wide")
st.title("📚 Newsletter Section Matches")

# Toggle between single-centroid and clustered views
mode = st.radio("🧠 Match Mode", ["Single Centroid", "Clustered"], horizontal=True)
MATCH_DIR = SINGLE_DIR if mode == "Single Centroid" else CLUSTER_DIR

# Gather section match files
section_files = sorted(Path(MATCH_DIR).glob("*.json"))
if not section_files:
    st.warning(f"No match files found in `{MATCH_DIR}`")
    st.stop()

section = st.selectbox("🗂️ Select Section", [f.stem.upper() for f in section_files])
file_path = next(f for f in section_files if f.stem.upper() == section)

# Load and render matches
with open(file_path) as f:
    matches = json.load(f)

st.subheader(f"Top {len(matches)} {mode.lower()} matches for: `{section}`")

for m in matches:
    st.markdown("---")
    if mode == "Clustered":
        st.markdown(f"**Distance**: {m['cosine_distance']} | **Cluster**: {m['cluster_id']}")
    else:
        st.markdown(f"**Distance**: {m['cosine_distance']}")
    st.markdown(f"[🔗 {m['url']}]({m['url']})")
    st.code(m["summary"], language="html")

