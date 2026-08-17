# Production worker / scheduler design

## Chosen mechanism (simplest safe existing pattern)

**Supervisor CLI + resume ticket**, invokable by any host scheduler.

```bash
# Normal build process (self-driving within runtime bounds)
ENABLE_VERIFIED_CENSUS_WRITES=1 npm run census:v4-full-build

# Auto-resume after INFRASTRUCTURE_RUNTIME_BOUNDARY
npm run census:v4-full-build-supervisor
```

Ticket file: `data/research-engine-v2/census-autopilot-v4-full-universe/50-auto-resume-ticket.json`

## Host options (use what you already have)

1. **Windows Task Scheduler** — every 5–15 min run `npm run census:v4-full-build-supervisor` from repo root with env loaded.
2. **cron** (Linux/macOS/CI runner) — same command.
3. **Railway / container** — if a worker service already runs Node jobs, point it at the supervisor; no new architecture required.
4. Existing `census:autopilot --resume` remains for V1 enrichment queues; **V4 footprint build uses the V4 supervisor**.

## Rules

- `resume: false` when COMPLETE or hard BLOCKED.
- Respect `next_work_scheduled_at` (no API hammering).
- Joan is **not** required to issue npm for normal continuation once supervisor is scheduled once.
- Process boundary ≠ BUILD COMPLETE when actionable work remains.
