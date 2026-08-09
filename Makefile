.PHONY: install seed run test reset
install:
	python -m pip install -r requirements.txt
seed:
	python scripts/seed_demo.py
run:
	uvicorn app.main:app --reload

test:
	pytest -q
reset:
	rm -f data/opsweave.db
	python scripts/seed_demo.py
