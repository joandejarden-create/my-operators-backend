# Arbor Lodging (CALA) — Operator Explorer seed

**Status:** Seeded to Airtable (new-base schema) on 2026-06-03.

## Record to use

| Role | Record ID | Notes |
|------|-----------|--------|
| **Operator Setup / Explorer (use this)** | `recF5Z87OAqFgndoq` | `company_name`: Arbor Lodging (CALA) — full new-base child tables |
| Legacy company row (do not use for Explorer) | `rectkHHTWMc6p4i63` | Old **Company Name** schema on same Master table — writer cannot update |

## Open in app

- **Operator Setup:** `/third-party-operator-setup-new-two.html?recordId=recF5Z87OAqFgndoq`
- **Operator DNA / Explorer:** `/operator-dna-profile.html?recordId=recF5Z87OAqFgndoq`

## What was populated

- **486** form fields from `scripts/arbor-cala-form-inventory.json` (research-based copy)
- **Operating Platform** child rows (`fixtures/operator-operating-explorer-arbor-cala.json`)
- **Engagement & Reporting** child rows (`fixtures/operator-engagement-explorer-arbor-cala.json`)
- **Leadership Team Members** (4) from `scripts/arbor-cala-leadership-restore.json`
- **Brands linked:** Courtyard by Marriott, Hilton Garden Inn, Hampton by Hilton, Hyatt Place, Holiday Inn Express, Kimpton Hotels

## Research basis (public sources)

- [Arbor Lodging — Platforms / CALA](https://www.arborlodging.com/platforms) — Mexico City hub, approved operator for Marriott, Hilton, Hyatt, IHG
- [Hotel Investment Today — Arbor Lodging](https://www.hotelinvestmenttoday.com/Development/Owners/What-makes-sense-today-for-Arbor-Lodging) — ~34 hotels / 4,800 rooms, third-party growth in Mexico, Cabo contract
- [Arbor press — Christian Hutchinson, CALA BD](https://www.arborlodging.com/press) — Nov 2025 appointment

Signals and KPIs use **Not Measured / N/A** where Arbor does not publish benchmarks (audit pass rate, reflag weeks, budget variance, etc.).

## Re-run / refresh

```bash
node scripts/export-arbor-cala-form-inventory.mjs recF5Z87OAqFgndoq
node scripts/apply-arbor-cala-inventory-to-airtable.mjs recF5Z87OAqFgndoq
```

Create another new master (if needed): `node scripts/apply-arbor-cala-inventory-to-airtable.mjs --create-new-master`

## Manual QA

1. Open Setup with `recF5Z87OAqFgndoq` — Company Profile shows **Arbor Lodging (CALA)** and Mexico/CALA narrative.
2. Brand tab — portfolio mix JSON / repeaters; signals mostly N/A except franchise align **High**.
3. Operating Platform — pillar tiles present (not empty JSON).
4. Explorer DNA — leadership cards for Bonthala, Patel, Hutchinson, DeGuia.
5. Confirm Legal/Comms review before owner-facing publication.

## Known limitations

- Footprint geo counts are **illustrative** CALA-scale (2 existing / 2 pipeline hotels in CALA grid) — replace with real portfolio data when available.
- Portfolio mix % is **modeled** from brand-family weighting, not audited Arbor disclosure.
- Case studies and diligence Q&A left empty (no public CALA case study pack).
- Leadership platform child table not seeded (0 rows) — optional follow-up fixture.
