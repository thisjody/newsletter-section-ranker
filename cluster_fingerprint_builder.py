#!/usr/bin/env python
"""
Cluster historical embeddings per section and store multiple centroids
to use as fingerprints for better semantic coverage.
"""

import os
import duckdb
import dotenv
import numpy as np
from collections import defaultdict
from sklearn.cluster import KMeans
from tqdm import tqdm

# Load config from .env
dotenv.load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/newsletter_embeddings.duckdb")
LINK_TABLE = os.getenv("CANDIDATE_TABLE", "link_embeddings")
CLUSTER_TABLE = os.getenv("CLUSTER_FINGERPRINT_TABLE", "section_cluster_fingerprints")
NUM_CLUSTERS = int(os.getenv("SECTION_CLUSTER_K", 3))
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", 768))

# Connect to DuckDB
con = duckdb.connect(DUCKDB_PATH, config={"enable_external_access": True})
con.execute("INSTALL vss; LOAD vss;")

# Drop and create the output table
con.execute(f"DROP TABLE IF EXISTS {CLUSTER_TABLE}")
con.execute(f"""
CREATE TABLE {CLUSTER_TABLE} (
    section TEXT,
    cluster_id INTEGER,
    embedding DOUBLE[{EMBEDDING_DIM}]
)
""")

# Collect and group historical embeddings by section
rows = con.execute(f"SELECT section, embedding FROM {LINK_TABLE} WHERE section != 'CANDIDATE'").fetchall()
section_vectors = defaultdict(list)

for section, emb in rows:
    if emb is not None:
        section_vectors[section.upper()].append(np.array(emb))

# Cluster and insert
print(f"📡 Generating clustered fingerprints with k={NUM_CLUSTERS} into table `{CLUSTER_TABLE}`")

for section, vectors in tqdm(section_vectors.items(), desc="🔁 Clustering sections"):
    matrix = np.stack(vectors)
    if len(matrix) < NUM_CLUSTERS:
        # fallback to mean if not enough samples
        centroid = matrix.mean(axis=0)
        con.execute(f"""
        INSERT INTO {CLUSTER_TABLE} (section, cluster_id, embedding)
        VALUES (?, ?, ?)
        """, [section, 0, centroid.tolist()])
    else:
        km = KMeans(n_clusters=NUM_CLUSTERS, random_state=42, n_init="auto")
        km.fit(matrix)
        for i, center in enumerate(km.cluster_centers_):
            con.execute(f"""
            INSERT INTO {CLUSTER_TABLE} (section, cluster_id, embedding)
            VALUES (?, ?, ?)
            """, [section, i, center.tolist()])

print(f"✅ Clustered fingerprints inserted into `{CLUSTER_TABLE}`")

