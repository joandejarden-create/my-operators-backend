# Scheduler Design

## Finding

**No in-repo cron / job runner / scheduled GitHub Actions** for product jobs.
Reuse pattern: **npm scripts + OS Task Scheduler / external cron** (same as quiet-sequential BE audits).

## Recommended initial cadence

| Cadence | Run type | Cohort | Why |
|---------|----------|--------|-----|
| **Daily** | `daily_lightweight` | Hotel Indigo + Kimpton — Mexico (~16–40) | ~3s runtime, $0, high Pipeline→Open value |
| **Weekly** | `weekly_integrity` | + Choice/Radisson sample + Hilton MX sample | Directory gaps + identity |
| **Monthly** | `monthly_activation` | Inactive/Under Review activation packs | Completeness, not maintenance |

## How to schedule (ops)

```bash
# Daily (Windows Task Scheduler / cron)
npm run research-engine-v2:shadow-operations-v1 -- --run-type daily_lightweight
```

Do **not** create duplicate schedulers inside Node until an org-standard job runner exists.
