mkdir -p runtime
docker build -t flow-sweep-bot .
docker rm -f flow-sweep-bot powerbar-bot 2>/dev/null || true
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