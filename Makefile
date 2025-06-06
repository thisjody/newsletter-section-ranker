check-env:
	@echo "🔍 Checking for .env file..."
	@test -f .env || (echo "❌ Missing .env file" && exit 1)

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

fingerprint: check-env
	@echo "🧠 Generating section fingerprints..."
	python fingerprint_builder.py

cluster-fingerprints:
	@echo "🔁 Clustering historical section embeddings..."
	python cluster_fingerprint_builder.py

annotate: check-env
	@echo "🔍 Annotating candidates..."
	python annotate_candidates_with_sections.py

cluster-annotate: check-env
	@echo "🔍 Annotating candidates using clustered fingerprints..."
	python annotate_candidates_with_clusters.py

dump-json: check-env
	@echo "📝 Dumping per-section match JSONs..."
	python annotate_candidates_with_sections.py --per_section_json_dir section_matches

dump-cluster-json: check-env
	@echo "📝 Dumping per-section clustered match JSONs..."
	python annotate_candidates_with_clusters.py --per_section_json_dir section_cluster_matches

inspect: check-env
	@echo "👁️ Inspecting candidate-section match results..."
	python inspect_matches.py

run-dashboard: check-env
	@echo "🚀 Launching Streamlit dashboard..."
	streamlit run candidate_db.py

