# wee-events.rs — Agent Instructions

## Workflow First

This repository uses the Superpowers workflow. Agents should explicitly use the relevant skills before acting.

Key skills for this repo:

- `using-superpowers` for every new conversation.
- `brainstorming` before designing or changing APIs, architecture, or behavior.
- `writing-plans` after design approval and before implementation on multi-step work.
- `test-driven-development` for feature and bugfix implementation.
- `systematic-debugging` when investigating failures or unexpected behavior.
- `verification-before-completion` before claiming work is done.
- `requesting-code-review` before handing off substantial implementation work.
- `receiving-code-review` before applying review feedback.

If a `jj` or `jujutsu` skill is available in the current harness, use it for repository workflow guidance. If not, use the `jj` CLI directly and follow the rules below.

## Version Control — jj First

This repository is managed with `jj`. Prefer `jj` for status, history, diff, and commit operations.

Common commands:

```bash
jj status
jj diff --git
jj log -n 10
jj show
jj workspace list
```

Rules:

- Prefer `jj` over `git` for normal repository workflow.
- Use `git` only for compatibility gaps where `jj` is not the right tool.
- Avoid interactive commands that open editors or pagers.
- For isolated parallel work, prefer `jj workspace add` over git worktrees.
- Check the working copy state before making changes and again before finishing.

## Build And Test

Tooling is managed through `mise`. Prefer `mise exec --` for Rust commands.

```bash
mise exec -- just fmt
mise exec -- just check
mise exec -- cargo test --workspace --all-features
```

Before finishing substantial work, run the relevant verification commands and report what was actually executed.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## Project Notes

- Keep core interfaces transport-agnostic. Transport concerns like JSON or Restate-specific wiring belong in adapters, not in the core service abstractions.
- Follow existing crate boundaries unless the task explicitly requires restructuring them.
- Prefer focused changes over broad refactors.
