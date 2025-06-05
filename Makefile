check-env:
	@echo "🔍 Checking for .env file..."
	@test -f .env || (echo "❌ Missing .env file" && exit 1)

install:
	pip install -r requirements.txt

fingerprint: check-env
	@echo "🧠 Generating section fingerprints..."
	python fingerprint_builder.py

annotate: check-env
	@echo "🔍 Annotating candidates..."
	python annotate_candidates_with_sections.py

dump-json: check-env
	@echo "📝 Dumping per-section match JSONs..."
	python annotate_candidates_with_sections.py --per_section_json_dir section_matches

inspect: check-env
	@echo "👁️ Inspecting candidate-section match results..."
	python inspect_matches.py
