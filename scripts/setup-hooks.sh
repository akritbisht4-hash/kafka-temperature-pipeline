#!/usr/bin/env bash
set -e

echo "Configuring git hooks directory..."
git config core.hooksPath .githooks
chmod +x .githooks/pre-push
echo "✅ Git pre-push hook active! Tests will be run before every 'git push'."
