#!/bin/sh

echo "🔗 Setting up Git hooks..."

HOOKS_DIR=".github/hooks"
GIT_HOOKS_DIR=".git/hooks"

mkdir -p "$GIT_HOOKS_DIR"

for HOOK in "$HOOKS_DIR"/*; do
    HOOK_NAME=$(basename "$HOOK")
    ln -sf "../../$HOOK" "$GIT_HOOKS_DIR/$HOOK_NAME"
    echo "✅ Hook '$HOOK_NAME' installed."
done

echo "🎉 Git hooks setup complete!"