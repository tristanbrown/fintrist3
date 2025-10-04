# AGENTS.GLOBAL.md

These are universal rules for AI agents across all repositories.  
They define baseline coding style principles that apply everywhere.  
Each repo supplements these with `AGENTS.PROJECT.md`.

---

## Universal Rules

1. **Implement only what was requested**  
   - Do not add unrequested features.  
   - Do not refactor without explicit instructions to do so.  

2. **Comments**  
   - Purpose: make code human-readable.  
   - Keep them concise and limited to clarifying the broad purpose of code blocks.  
   - Do not use comments as historical logs of edits.  
   - Omit comments when clear naming and structure make them unnecessary.  

3. **Readability through structure**  
   - Avoid excessive nesting.  
   - Use abstraction and modularity to keep top-level code concise and readable, preferably fitting on a single screen.  
   - Favor meaningful names for methods and variables so code is self-explanatory.  

4. **DRY (Don’t Repeat Yourself)**  
   - Do not duplicate logic.  
   - Use abstraction and modularity to ensure a single source of truth for any piece of logic.  

5. **Conciseness**  
   - Prefer as few lines of code as possible, but no less.  
   - Avoid unnecessary boilerplate, wrappers, or abstractions that don’t add clarity.  
   - Do not collapse too much logic into single lines if it reduces readability or makes debugging harder.  
   - Simplify when possible, but not at the expense of readability.  

6. **Soundness over hacks**  
   - Do not use brittle or hacky workarounds.  
   - Prefer solutions that are maintainable, robust, and aligned with project conventions.  
   - If a proper solution is unclear, ask for clarification instead of guessing.  

7. **Portability through modularity**  
   - Structure logic into modules, functions, or classes that can be reused in multiple contexts.  
   - Avoid embedding universal logic in places where it cannot be reused.  
   - Portable logic should be clean, general, and free of project-specific assumptions unless explicitly required.  

8. **Separation of concerns**  
   - Keep distinct layers of the codebase isolated:  
     - UI layout separate from widget logic  
     - Widget logic separate from data processing  
     - Data processing separate from database access  
   - Each layer should be as self-contained and portable as possible.  
   - Cross-layer dependencies should be minimal, explicit, and well-defined.  

9. **Consistency and consolidation**  
   - Reuse existing logic and abstractions whenever possible.  
   - Do not reimplement similar solutions in multiple places; abstract them into a single, universal form.  
   - Introducing new patterns is welcome if they clearly improve clarity, adaptability, or replace outdated/messy approaches.  

10. **Thoughtful use of dependencies**  
    - External dependencies are allowed if they are reliable, well-maintained, and reduce workload significantly.  
    - Prefer built-in features or existing project utilities when they serve the purpose well.  
    - Avoid unnecessary or redundant dependencies.  
    - If adding a dependency, explain why it’s the right tool.  
    - Ensure dependencies are flexible enough to adapt to future needs.

11. **Isolated environments**  
    - Do not install dependencies directly to the host system.  
    - Always use isolated environments (e.g., venv, conda, or language-specific equivalents).  
    - Containerized environments (e.g., Docker) are an exception: system-level installs inside a container are acceptable.  
    - Ensure setups are portable and reproducible across machines.  

---

## Philosophical Note
These rules align with the *Zen of Python* (PEP 20), whose principles are broadly applicable across languages:  
- Readability counts.  
- Simple is better than complex.  
- Flat is better than nested.  
- There should be one obvious way to do it.  

Agents should interpret these rules in the same spirit: clarity, simplicity, and consistency matter most.

---

## Interaction Flow
1. Read this file (`AGENTS.GLOBAL.md`) for universal rules.  
2. Then read `AGENTS.PROJECT.md` for repo-specific architecture and focus.  
3. If rules conflict, **project-specific rules override global rules**.  
