#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p runtime

if [[ "${USE_HOST_DOCKER_CONFIG:-0}" != "1" ]]; then
	DOCKER_CONFIG="$SCRIPT_DIR/runtime/docker-config"
	export DOCKER_CONFIG
	mkdir -p "$DOCKER_CONFIG"
	if [[ ! -f "$DOCKER_CONFIG/config.json" ]]; then
		printf '{}\n' > "$DOCKER_CONFIG/config.json"
	fi
fi

echo "Using Docker config: $DOCKER_CONFIG"
echo "Building flow-sweep-bot image..."
docker build -t flow-sweep-bot .
docker rm -f flow-sweep-bot powerbar-bot 2>/dev/null || true
echo "Starting flow-sweep-bot. Dashboard: http://localhost:${BOT_DASHBOARD_PORT:-8765}"
docker run --rm --name flow-sweep-bot \
	--env-file .env \
	-e AWS_PROFILE="${AWS_PROFILE:-trading_bot}" \
	-e AWS_REGION="${AWS_REGION:-us-east-2}" \
	-e BOT_DASHBOARD_HOST="0.0.0.0" \
	-e BOT_DASHBOARD_PORT="${BOT_DASHBOARD_PORT:-8765}" \
	-p "${BOT_DASHBOARD_PORT:-8765}:${BOT_DASHBOARD_PORT:-8765}" \
	-v "$HOME/.aws:/root/.aws:ro" \
	-v "$(pwd)/runtime:/app/runtime" \
	flow-sweep-bot