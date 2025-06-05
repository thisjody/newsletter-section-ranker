import os
import duckdb
import dotenv
import numpy as np
from tqdm import tqdm
from collections import defaultdict

dotenv.load_dotenv()

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/newsletter_embeddings.duckdb")
LINK_TABLE = os.getenv("CANDIDATE_TABLE", "link_embeddings")
FINGERPRINT_TABLE = os.getenv("FINGERPRINT_TABLE", "section_fingerprints")

con = duckdb.connect(DUCKDB_PATH, config={"enable_external_access": True})
con.execute("INSTALL vss; LOAD vss;")
con.execute(f"DROP TABLE IF EXISTS {FINGERPRINT_TABLE}")
con.execute(f"""
CREATE TABLE {FINGERPRINT_TABLE} (
    section TEXT PRIMARY KEY,
    embedding FLOAT[768]
)
""")

rows = con.execute(f"SELECT section, embedding FROM {LINK_TABLE}").fetchall()
section_vectors = defaultdict(list)
for section, emb in rows:
    if emb is not None:
        section_upper = section.upper()
        if section_upper != "CANDIDATE":
            section_vectors[section_upper].append(np.array(emb))

for section, vectors in tqdm(section_vectors.items(), desc="🧠 Fingerprinting sections"):
    matrix = np.stack(vectors)
    fingerprint = matrix.mean(axis=0).tolist()
    con.execute(f"""
    INSERT INTO {FINGERPRINT_TABLE} (section, embedding)
    VALUES (?, ?)
    """, [section, fingerprint])
print(f"✅ Inserted {len(section_vectors)} section fingerprints into {FINGERPRINT_TABLE}")
