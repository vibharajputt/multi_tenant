#!/usr/bin/env bash
set -euo pipefail
docker compose run --rm api python -m app.verify
