# HE-INTEL-001 P3B-M1 Live Activation — Founder corrections (PR #439)

**PR:** https://github.com/joandejarden-create/my-operators-backend/pull/439  
**Prior reviewed HEAD:** `434c78d213b2eaddcd4dcfb27c2053497e781e41`  
**Corrected HEAD (tip):** `6797cc04bc915a8236f4c253e68cbadcc1cdfe06` (includes origin/main FF merge after fix commit `f23ea2a33`)  
**Status:** Corrections applied on same PR — AWAITING_INTEGRATION_CONTROLLER  
**Paid $5:** NOT AUTHORIZED

## Corrections delivered

1. **Durable ledger fail-closed** — corrupt/unreadable/partial/.tmp/integrity-mismatch/cap-mismatch → fatal; no empty usable ledger; negative/nonfinite charges rejected; reconstruction failures fatal; reserved amounts preserved on PENDING_RECONCILE.
2. **Dispatch claim/lease** — RESERVED ≠ authorize dispatch; `claimDispatch` exclusive across workers; second process gets IN_FLIGHT / settled / pending.
3. **Sealed gateway enforcement** — execute uses sealed `adapter.call` only; `__rawCall` removed; `__rawTransport` throws; auth cancelled after execute.
4. **Multi-process tests** — independent Node workers for same op ID (exactly one dispatch), $5 run cap, $2.50 per-hotel cap.
5. **Discovery candidates** — Exa/Parallel search → unverified candidates; Parallel Task create≠complete; poll/resume by `run_id`; no duplicate task create.
6. **Financial** — conservative estimates + headroom; inline Exa contents unbounded; native USD UNKNOWN fail-closed; expected $0.204 is **not** a max-charge guarantee; binding caps remain $5 / $2.50.

## Tests

```bash
npm run test:he-intel-001-p3b-m1-live-activation-v1   # 17 PASS
npm run test:he-intel-001-p3b-m1-readiness-v1
npm run test:he-intel-001-ownership-research-objective-v1
npm run test:he-intel-001-p3a-research-benchmark-v1
npm run test:he-intel-001-research-engine-v1
npm run test:he-intel-001-p2-airtable-graph-v1
npm run dry-run:he-intel-001-p3b-m1-live-activation
npm run dealality:ci-precheck
```

## Remaining live-execution limitations

- Proposed $5 spend still NOT AUTHORIZED — entrypoint refuses paid dispatch.
- Method recommendation deferred until authorized live results exist.
- Account plan-effective pricing not confirmed (presence-only probes).
- Native remains offline-zero for readiness only; not a paid arm.

**STOP** — no merge from this chat; Integration Controller owns merge + HE post-merge sync after CI green.

`AWAITING_INTEGRATION_CONTROLLER`
