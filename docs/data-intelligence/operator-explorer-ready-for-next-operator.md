# Operator Explorer — ready for next operator

Use this checklist when quality baselines (Arbor + Hotel Equities) are frozen and you are about to start the next Operator Explorer Tab Factory build.

## Binding signal

```bash
npm run test:operator-explorer-os
npm run operator-explorer-os -- --source=merged --dry-run
```

`canStartNextOperatorExplorer=true` means:

1. Both golden baselines clear Tab Factory gates (with remediation overlay preview if needed)
2. Factory queue has a next operator

**Next operators in Tab Factory:**

| Wave | Operator | Record | Status |
| --- | --- | --- | --- |
| A | GHL Hoteles | `reciI2tYQBfMoMK9G` | `factory_ready` — founder review |
| A | Aimbridge LATAM | `recGWxIJqnYHkJZFD` | `factory_ready` — founder review |
| B | Tafer → Atlantica (+ Highgate, GSF, Arriva, Brittain) | see batch doc | Masters + scaffolds — **content next** |
| E | OxoHotel | `rectsHzacZDFTH1Ze` | Master + linked Setup tabs — Tab Factory content next |
| E | Grupo Marta Hospitality | `recuEDrp6oeJIEuRX` | Master + linked Setup tabs — Tab Factory content next |
| E | Grupo Iberostar | `recwEHUotSGpfkZEJ` | Master + linked Setup tabs — Tab Factory content next |
| — | Arbor Lodging | `recF5Z87OAqFgndoq` | **quality baseline** (not factory) |

Batch detail: `docs/data-intelligence/operator-explorer-batch-2026-07-24.md`

**Next content build (queue head):** Tafer Hotels & Resorts (`recJ6NPSYveCTo3At`)

Then: Viento Sur (`recZPHT2zqc8K6itx`) — confirm official domain before factory-init.

## Start / refresh factory content

```bash
# Confirm OS readiness (goldens + factory)
npm run operator-explorer-os -- --operators arbor-lodging-cala,hotel-equities-cala,ghl-hoteles,aimbridge-latam --source=merged --dry-run

# Materialize Tab Factory fixtures (dry-run first)
npm run operator-explorer-factory-content-materialize -- --operators ghl-hoteles,aimbridge-latam --dry-run
npm run operator-explorer-factory-content-materialize -- --operators ghl-hoteles,aimbridge-latam --apply --approve-operator-factory-content-materialize

# Gates
npm run operator-explorer-tab-factory-audit -- --operators ghl-hoteles,aimbridge-latam --source=fixtures --dry-run
npm run operator-explorer-section-pattern-parity-audit -- --operators ghl-hoteles,aimbridge-latam --source=fixtures --dry-run
```

**Aimbridge Master:** created 2026-07-24 via `npm run create-aimbridge-latam-operator-master` (`company_name` + `submission_status=Draft`).

## Overlay / Airtable

```bash
npm run operator-explorer-overlay-airtable-apply -- --dry-run
```

Live Airtable overlay apply is **blocked in v1** (fixture overlay + preview only). Goldens require `--confirm-baseline-revision` for any apply gate touching them.

## Related docs

- `docs/data-intelligence/operator-explorer-protected-baseline-rules.md`
- `docs/data-intelligence/operator-explorer-tab-factory-build-operation.md`
- `docs/data-intelligence/operator-explorer-mandatory-release-gates.md`
- `lib/partner-intelligence/operator-explorer-factory-queue.js`
