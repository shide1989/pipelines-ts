# pipelines

A minimal, durable workflow engine for TypeScript/Bun. Workflows are plain async
functions — durable checkpointing, replay, and long-lived timers are handled
transparently via `Proxy` + `AsyncLocalStorage`. No compiler, no magic strings.

> Status: **scaffold**. Public API surface and types are in place; runtime logic
> is stubbed (`throw new Error("Not implemented")`). See `SPEC PIPLINES V0.5`.

## Layout

| Path | What |
|------|------|
| `packages/runtime` | Durable execution engine (platform-agnostic core, Bun host) |
| `packages/pipelines` | User-facing package — re-exports the runtime |
| `examples/agentic` | Primary demo: task → batch inference → durable poll → validate |
| `examples/onboarding` | Hello-world: signup → welcome → 7-day sleep → check-in |
| `schema.sql` | PostgreSQL schema (three tables) |
| `docker-compose.yml` | Local Postgres |

## Dev

```bash
pnpm install
docker compose up -d        # Postgres on :5432, schema auto-applied
pnpm check                  # biome format + lint + organize imports
pnpm typecheck              # tsc --noEmit across the workspace
```
