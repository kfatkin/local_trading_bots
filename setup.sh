#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

mkdir -p runtime
DASHBOARD_PORT="${BOT_DASHBOARD_PORT:-8765}"
DASHBOARD_URL="http://127.0.0.1:${DASHBOARD_PORT}"
FOLLOW_LOGS="${BOT_FOLLOW_LOGS:-0}"
export DOCKER_BUILDKIT="${DOCKER_BUILDKIT:-0}"

if [[ "${USE_HOST_DOCKER_CONFIG:-0}" != "1" ]]; then
	DOCKER_CONFIG="$SCRIPT_DIR/runtime/docker-config"
	export DOCKER_CONFIG
	mkdir -p "$DOCKER_CONFIG"
	if [[ ! -f "$DOCKER_CONFIG/config.json" ]]; then
		printf '{}\n' > "$DOCKER_CONFIG/config.json"
	fi
else
	DOCKER_CONFIG="${DOCKER_CONFIG:-$HOME/.docker}"
fi

stop_local_bot_containers() {
	echo "Stopping existing local bot containers..."
	docker rm -f flow-sweep-bot powerbar-bot 2>/dev/null || true
	local published_containers
	published_containers="$(docker ps --filter "publish=${DASHBOARD_PORT}" --format '{{.ID}}' 2>/dev/null || true)"
	if [[ -n "$published_containers" ]]; then
		echo "$published_containers" | xargs -r docker rm -f >/dev/null
	fi
}

free_dashboard_port() {
	local pids=""
	if command -v lsof >/dev/null 2>&1; then
		pids="$(lsof -tiTCP:"$DASHBOARD_PORT" -sTCP:LISTEN 2>/dev/null || true)"
	elif command -v fuser >/dev/null 2>&1; then
		pids="$(fuser "${DASHBOARD_PORT}/tcp" 2>/dev/null || true)"
	else
		echo "No lsof or fuser found; skipping local process cleanup for port ${DASHBOARD_PORT}."
		return
	fi

	if [[ -n "$pids" ]]; then
		echo "Freeing localhost port ${DASHBOARD_PORT} from process(es): $pids"
		printf '%s\n' $pids | xargs -r kill 2>/dev/null || true
	fi
}

wait_for_dashboard() {
	if ! command -v curl >/dev/null 2>&1; then
		echo "curl is not installed; skipping dashboard health check."
		return 0
	fi

	for _attempt in {1..30}; do
		if curl -fsS --max-time 2 "${DASHBOARD_URL}/health" >/dev/null 2>&1; then
			echo "Dashboard is ready: ${DASHBOARD_URL}"
			return 0
		fi

		if ! docker ps --format '{{.Names}}' | grep -qx 'flow-sweep-bot'; then
			echo "flow-sweep-bot exited before the dashboard became ready. Recent logs:"
			docker logs --tail 80 flow-sweep-bot 2>/dev/null || true
			return 1
		fi

		sleep 1
	done

	echo "Dashboard did not become ready at ${DASHBOARD_URL}/health. Recent logs:"
	docker logs --tail 80 flow-sweep-bot 2>/dev/null || true
	return 1
}

echo "Using Docker config: $DOCKER_CONFIG"
echo "Using Docker BuildKit: $DOCKER_BUILDKIT"
stop_local_bot_containers
free_dashboard_port
echo "Building flow-sweep-bot image..."
docker build -t flow-sweep-bot .
echo "Starting flow-sweep-bot detached. Dashboard: ${DASHBOARD_URL}"
docker run -d --name flow-sweep-bot \
	--env-file .env \
	-e AWS_PROFILE="${AWS_PROFILE:-trading_bot}" \
	-e AWS_REGION="${AWS_REGION:-us-east-2}" \
	-e BOT_DASHBOARD_HOST="0.0.0.0" \
	-e BOT_DASHBOARD_PORT="${DASHBOARD_PORT}" \
	-p "${DASHBOARD_PORT}:${DASHBOARD_PORT}" \
	-v "$HOME/.aws:/root/.aws:ro" \
	-v "$(pwd)/runtime:/app/runtime" \
	flow-sweep-bot >/dev/null

wait_for_dashboard

echo "View logs: docker logs -f flow-sweep-bot"
echo "Stop bot:  docker rm -f flow-sweep-bot"

if [[ "$FOLLOW_LOGS" == "1" ]]; then
	docker logs -f flow-sweep-bot
fi