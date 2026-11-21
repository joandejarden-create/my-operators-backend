# Integration Controller — PR #503 handoff

## Status

**Product chat does not merge.** Integration Controller owns merge + post-merge sync.

| Item | Value |
|---|---|
| PR | https://github.com/joandejarden-create/my-operators-backend/pull/503 |
| Tip (pre-this-push) | `64077e881` (+ Agent closeout commit when pushed) |
| Mergeable | MERGEABLE |
| Merge state | BEHIND main — IC rebase/FF |
| CUSTOMER_BLOCKED | Yes |
| HPC / Meta apply | None |
| Agent default | `MANUAL_ESCALATION_ONLY` (auto OFF) |
| Lease | Deferred — incidental retain only |

## Gates to verify before merge

- Required CI green on tip
- Date-semantic gate tests
- Commercial target discovery tests (dual lists + lease deferral)
- Exa Agent default-off / manual auth tests
- Dual prospect lists present; HOLD excluded
- Research persistence CUSTOMER_BLOCKED

## Post-merge

```bash
npm run dealality:post-merge-sync -- --mode post-merge --stream he --pr 503
npm run dealality:post-merge-sync -- --mode post-merge --stream he --pr 503 --apply
```

Do **not** start a competing implementation PR before authorized merge.

Next wave after merge: Search-first global franchise + management discovery (see `NEXT_WAVE_GLOBAL_DISCOVERY_PLAN.md`).
