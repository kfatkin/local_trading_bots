mkdir -p runtime
docker build -t powerbar-bot .
docker rm -f powerbar-bot 2>/dev/null || true
docker run --rm --name powerbar-bot --env-file .env -v "$(pwd)/runtime:/app/runtime" powerbar-bot