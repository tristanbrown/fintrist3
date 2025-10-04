# Agent Instructions – Usage Guide

This repo holds `AGENTS.GLOBAL.md`, the universal rules for AI agents.  
Projects pull it into `.agents/` using **Git subtree (with squash)**.

## Add to a new project
    git remote add agent-instructions git@github.com:tristanbrown/agent-instructions.git
    git fetch agent-instructions
    git subtree add --prefix=.agents agent-instructions main --squash

## Update in an existing project
    git fetch agent-instructions
    git subtree pull --prefix=.agents agent-instructions main --squash

## Notes
- Projects also define `AGENTS.PROJECT.md` for repo-specific rules.  
- The root `AGENTS.md` should point to both `.agents/AGENTS.GLOBAL.md` and `AGENTS.PROJECT.md`.
