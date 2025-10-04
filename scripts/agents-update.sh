#!/usr/bin/env bash
set -euo pipefail

REMOTE_NAME=agent-instructions
REMOTE_URL=git@github.com:tristanbrown/agent-instructions.git
PREFIX_DIR=.agents
BRANCH=main

git remote add "$REMOTE_NAME" "$REMOTE_URL"
git fetch "$REMOTE_NAME"
git subtree pull --prefix="$PREFIX_DIR" "$REMOTE_NAME" "$BRANCH" --squash

echo "Agents subtree updated in $PREFIX_DIR from $REMOTE_NAME/$BRANCH." 
