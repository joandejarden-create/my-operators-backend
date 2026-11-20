# Public Contact Discovery 2.0 — Maritza certification gate

**Status:** READY FOR FOUNDER REVIEW (STOP — no full DB backfill yet)

## Root cause

`recoverPublicContactChannels` only fetched opportunity-attached news/project URLs. Maritza’s SIOP directory pages were never candidates, so extraction never ran on official sources. Extraction works once `info.jalisco.gob.mx` / `transparencia.info.jalisco.gob.mx` are seeded.

**Limiting stage:** 1+2+4 — no explicit person-channel search / insufficient query variation / missing authoritative staff-directory discovery (not DTO loss, not Airtable, not Surfe).

## Public-source search path (controlled cert)

1. Existing local opportunity contact (null email/phone).
2. Seed fixture candidate URLs (official directories).
3. Fetch + person-scoped extract via `recoverPublicContactChannels`.
4. Enrich office phone + extensions; classify `OFFICE_WITH_EXTENSION`.
5. Merge onto `primaryContact`; Surfe CTA cleared when both channels present.
6. Airtable upsert **dry-run only** (no apply / no Surfe / no paid search).

## Regression evidence

| Check | Result |
|-------|--------|
| Email | `maritza.medina@jalisco.gob.mx` |
| Phone | `+52 33 3819 2333` |
| Extensions | `42333` / `47542` |
| Phone kind | `OFFICE_WITH_EXTENSION` (not switchboard-as-direct) |
| Surfe CTA | `null` (was Find Email & Phone) |
| News-only control | no channels (proves prior gap) |
| Surfe / paid search calls | **0** |
| Authority | procurement route ≠ hotel-booking buyer |

Artifacts: `MARITZA_CERT.json`, `AUDIT.md`.

## Airtable

Dry-run proposal only — existing GDI research inventory target, idempotent key, `surfeDerived: false`. **No apply** pending founder authorization after this gate.

## Screenshots

| Asset | Path |
|-------|------|
| Presentation Surfe-last card | `screenshots/01-maritza-public-contact-card-surfe-last.png` |
| Local drawer comparison (BEFORE Find Email & Phone → AFTER email+office phone) | `screenshots/02-local-drawer-comparison-maritza-before-after.png` |

Canonical customer **publication package** (default `/app` without `?live=1`) still serves the staged snapshot **without** channels until authorized inventory write + gated republish. Research inventory / `?live=1` / drawer-comparison AFTER recover public channels correctly.

External Railway publication package: **not updated** this gate (no customer-publication expansion).

## Spend

Surfe: **0**. Paid web search: **0**. Airtable apply: **0**.

## Next (after founder OK)

1. Authorized Airtable write + read-back for Maritza.
2. Gated publication refresh so default `/app` + external share clear Surfe CTA.
3. Wire into weekly / research-target continuation with budget caps.
4. Restartable backfill batches + measurement.
5. Integration Controller merge + deploy.

**STOP — awaiting founder review at Maritza certification gate. No full DB backfill.**
