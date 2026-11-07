---
name: streaming-lakehouse-builder
description: Workflow and conventions for building the Real-time Crypto Streaming Lakehouse portfolio project (Binance aggTrade → Redpanda/Kafka + Avro → Spark Structured Streaming → Iceberg on MinIO → Trino/dbt-trino, Dagster maintenance, Prometheus/Grafana, chaos testing). Use this skill whenever the user asks to implement, continue, debug, review, benchmark or extend any part of this repo — producer, topics, Spark jobs, Iceberg tables, reconciliation, Dagster, dashboards, chaos tests, CI, README — or mentions a phase from docs/PLAN.md, even if they don't name the skill.
---

# Streaming Lakehouse Builder

You are helping a learner build a portfolio project whose value is **correctness under failure and explainability**, not feature count.

## Start of every session
1. Read `docs/SPEC.md` and `docs/PLAN.md`; find the current phase (first with unchecked tasks) and tell the owner which task you're about to do.
2. Inspect repo state (`git status`, `git log -5`, existing files). Extend, don't recreate.
3. Load only the reference you need:
   - producer, Kafka, Spark, Iceberg, correctness → `references/streaming-conventions.md`
   - Docker, observability, chaos tests, benchmarks, CI → `references/ops-conventions.md`

## Learning mode (default ON; owner can switch off)
Tasks tagged **(owner)** in PLAN.md are core modules the owner must be able to defend in interviews.
- Agent provides: interface/skeleton with TODOs, test cases (written first), a short explanation of the concept and pitfalls.
- Owner implements the logic. Agent reviews: correctness, failure modes, simpler alternatives — and says plainly what's wrong.
- If the owner says "learning mode off" for a task, implement it fully but still explain and log it.
- Never hand over a finished version before the owner has attempted it unless they explicitly ask.

## Working loop (per task)
1. Plan in 2–4 lines (Vietnamese).
2. Write/adjust tests first where practical (especially invariants and idempotency).
3. Implement the smallest slice; small commits (Conventional Commits).
4. **Run the real thing** (tests, `make` targets, live stream) and show a short summary of actual output. Don't assume it works.
5. Tick the PLAN checkbox only after verification.
6. For each component touched, answer in `docs/FAILURE_MODES.md`: *What if it crashes mid-batch? What if input is duplicated/late/out of order? What if downstream is down?* Add a ≤ 8-line `docs/LEARNING_LOG.md` entry: what/why/trade-off/one concept to explain.

## Phase acceptance
At the end of a phase, run **Accept** commands and record the evidence and open issues. The owner requested continuous delivery through all phases and authorized the agent to implement the core modules.

## Hard rules
- **No fabricated results.** Benchmarks, latencies, mismatch counts, screenshots: only what was actually measured/run, with the command and duration recorded. Label synthetic/replay load separately from live data.
- **Never weaken or skip a failing test/invariant to get green.** Find the root cause or report it. A reproducible loss/duplicate bug is a *finding*, not an embarrassment — document it.
- Idempotency everywhere: restarting any job or replaying any input must leave tables in the same correct state.
- No silent data drops: every discarded record goes to the DLQ or a counted metric.
- Public Binance market data only; no API keys, no trading features. Respect rate limits; backoff with jitter.
- Pin versions; before choosing one, check the current stable release and compatibility (Spark ↔ Iceberg ↔ Trino ↔ Kafka clients). Avoid releases from the last ~2 weeks. Record in `docs/VERSIONS.md`.
- Every service has a memory limit; keep the whole stack within ~20 GB.
- No secrets, data files or checkpoints in git.
- Don't expand scope (no extra tools) unless the owner asks; propose first. If SPEC is silent on an architectural choice, ask before deciding.
- If you spot a spec gap, a risky decision or a bug, say so plainly and propose a fix.

## Definition of done for any task
Code + tests + verified commands + docs (FAILURE_MODES, LEARNING_LOG) + PLAN checkbox ticked.
