# Shadow Operations V1 — Operating Architecture

## Principle

Research Engine V2 identifies **what changed** and **what needs human review**.
It does **not** decide what to write automatically.

```
Cohort config → Freshness adapters (IHG/Marriott/Choice/Hilton)
  → Source-state gate (Blocked/Failed ≠ proposals)
  → Dedup state (local only)
  → Steward queue (P0–P3)
  → Review packs
  → Steward decision
  → Existing governed write path (or NO SAFE WRITE PATH YET)
```

## Constraints

- No Webhound auto-call
- No Airtable writes from this engine
- No automatic brand activation
- No automatic image replacement
- Company Validated / PVQL / Tab Factory / freeze rules remain authoritative
