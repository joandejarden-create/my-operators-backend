# GDI Full E2E Hotel Template V1 — Operating Playbook

**Template ID:** `GDI_FULL_E2E_HOTEL_TEMPLATE_V1`  
**Contract:** `gdi-full-e2e-contract-v1.5`  
**Reference:** Bethesda Marriott V2 (`df8da65a1727…`)

## Principles

1. Research never silently changes customer-visible publication content.
2. Airtable is the mutable research/commercial registry; publication packages are immutable.
3. Feedback is outside the publication fingerprint.
4. Unknown hotel fields stay unknown — never fabricate need periods.
5. `?live=1` is research browse; default authenticated GDI is publication preview (same renderer as share).
6. Mode banners are diagnostic-only (`?gdiDiag=1`).

## Preflight (every hotel)

1. Confirm canonical census/HPC `hotelId`.
2. Ensure `config/group-demand-intelligence/hotels/<hotelId>.json` exists (create from schema; leave unknowns empty).
3. Dry-run inventory: `npm run gdi:research-storage-reconciliation-v1 -- --dry-run --hotel <hotelId>` (when hotel-scoped flags available) or assess existing data roots.
4. Run template gate: `npm run test:gdi-full-e2e-hotel-template-v1`.

## Execute Full E2E

```bash
npm run gdi:full-e2e -- --hotel <hotelId> --authorization-ref <ledger> --dry-run
```

Only after founder budget approval, re-run without `--dry-run` and with the appropriate `--authorize-*` flags documented in `reports/gdi-full-e2e-contract/OPERATOR_RUNBOOK.md`.

## Persist & preview

- Canonical writer path must keep presentation keys (`gdi-canonical-presentation-persistence-v1`).
- Founder preview: `/group-demand-intelligence.html?hotelId=<hotelId>`.
- Stage immutable package → approve exact fingerprint → production release via GitHub Actions `Dealality production release` (product=GDI).

## Do not

- Copy Bethesda presentation values into other hotels.
- Publish without fingerprint approval.
- Treat SPECULATIVE_DEMAND as customer-ready without support.
- Skip failed/missing gates (NOT VERIFIED ≠ PASS).
