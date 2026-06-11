# pipelines

A minimal, durable workflow engine for TypeScript/Bun. Workflows are plain async
functions — durable checkpointing, replay, and long-lived timers are handled
transparently via `Proxy` + `AsyncLocalStorage`. No compiler, no magic strings.

> Status: **Phase 1 + 2 implemented** (event-driven worker, durable sleep, per-step
> retry, hybrid log). Streaming (Phase 3) and polish (Phase 4) are deferred. The
> runtime is DB-client agnostic; the `agentic` example supplies a Drizzle/porsager
> client. See `SPEC PIPLINES V0.6`.

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
bun test packages/runtime   # integration tests — need the Postgres above
```
