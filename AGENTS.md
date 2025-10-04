# AGENTS.md

This file references two layers of agent instructions.  
Both must be read and applied together when working in this repository.

1. **.agents/AGENTS.GLOBAL.md**  
   - Universal coding principles shared across all projects.  
   - Maintained in the `agent-instructions` repo and synced here.  

2. **AGENTS.PROJECT.md**  
   - Project-specific rules, architecture details, and current focus.  
   - Overrides global rules if there is any conflict.  

---

## Rule of Interpretation
- Agents must apply both files in combination.  
- If universal and project-specific rules conflict, the **project-specific rules take priority**.  


## Initialization and Updates
- The `.agents/` folder is maintained via Git subtree (squash) from `agent-instructions`.
- To initialize or update, use the scripts in `scripts/`.
- Scripts default to SSH; they do not enforce a clean tree.
- Git subtree operations may still require a clean working tree.


## Commands
- Init: `scripts/agents-init.sh` or `scripts/agents-init.ps1`
- Update: `scripts/agents-update.sh` or `scripts/agents-update.ps1`
  - Uses SSH by default.
