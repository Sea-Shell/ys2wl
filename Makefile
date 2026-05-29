.PHONY: sync test lint format check clean docker

sync:
	uv sync --dev

test:
	uv run pytest tests/ -v --tb=short

lint:
	uv run ruff check src/ tests/

format:
	uv run ruff format src/ tests/

check:
	uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/

docker:
	docker build -t ys2wl .

clean:
	rm -rf .venv .ruff_cache .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name '*.egg-info' -exec rm -rf {} + 2>/dev/null || true
	rm -f .python-version
