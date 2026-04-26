#!/usr/bin/env bash
set -e

REPO_URL="${1:-}"
INSTALL_DIR="/opt/mlops"
DAGSTER_HOME="/opt/dagster_home"

if [ -z "$REPO_URL" ]; then
    echo "Usage: bash setup.sh <git-repo-url>"
    exit 1
fi

echo "==> Installing uv"
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

echo "==> Cloning repo to $INSTALL_DIR"
git clone "$REPO_URL" "$INSTALL_DIR"
cd "$INSTALL_DIR"

echo "==> Installing dependencies"
uv sync

echo "==> Creating .env"
cp .env.example .env
echo ""
echo "Edit $INSTALL_DIR/.env and set MOTHERDUCK_TOKEN, ENV, and DAGSTER_HOME=$DAGSTER_HOME"
echo "Then press Enter to continue."
read -r

echo "==> Creating Dagster home directory"
mkdir -p "$DAGSTER_HOME"

echo "==> Installing systemd services"
cp deploy/dagster-webserver.service /etc/systemd/system/
cp deploy/dagster-daemon.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now dagster-webserver dagster-daemon
systemctl restart dagster-webserver dagster-daemon

echo ""
echo "Done. Dagster is running."
echo "  Status:  systemctl status dagster-webserver dagster-daemon"
echo "  UI:      http://$(curl -s ifconfig.me):3000"
echo "  Or SSH tunnel: ssh -L 3000:localhost:3000 root@$(curl -s ifconfig.me)"
