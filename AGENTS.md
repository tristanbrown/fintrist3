**Agent Instructions**
- Global rules: `.agents/AGENTS.GLOBAL.md`
- Project rules: `AGENTS.PROJECT.md`

Notes
- The `.agents/` folder is maintained via Git subtree (squash) from `agent-instructions`.
- To initialize or update, use the scripts in `scripts/`.
- Scripts default to SSH; they do not enforce a clean tree.
- Git subtree operations may still require a clean working tree.

Precedence
- If rules conflict, `AGENTS.PROJECT.md` overrides `.agents/AGENTS.GLOBAL.md` by default.

Commands
- Init: `scripts/agents-init.sh` or `scripts/agents-init.ps1`
- Update: `scripts/agents-update.sh` or `scripts/agents-update.ps1`
  - Uses SSH by default.
