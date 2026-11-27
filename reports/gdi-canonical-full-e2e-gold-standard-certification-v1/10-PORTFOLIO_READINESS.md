# Product-wide GDI hotel readiness (read-only)

**Scope:** Existing GDI hotels — no paid portfolio re-research this cycle.  
**Reference certified tip:** Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)

## Assessment method

1. Inventory bundled publication packages under `lib/group-demand-intelligence/bundled-publication-packages/`.
2. Compare each hotel against gold-standard gates: five-stage runners present, STAGED pointer, contact channel research annotation, Findymail Stage 4 eligibility path, Airtable opportunity table mapping.
3. Do **not** auto-run paid Exa/Findymail across the portfolio.

## Rollout priority (remediation)

| Priority | Action | Hotels |
|---|---|---|
| P0 | Keep Wyndham as gold-standard reference; continue Findymail email gaps + phone/LinkedIn public research | Wyndham only for live enrichment this cycle |
| P1 | Run gold-standard certification dry-run per hotel (`gdi-canonical-full-e2e-gold-standard-certification-v1.mjs` generalized) | Hotels with STAGED packages |
| P2 | Stage 4 Findymail for email gaps after Airtable recovery | Hotels with verified named contacts missing email |
| P3 | WHO/HOW alternative-entry hydration into STAGED tip fields | Hotels where alts exist only in research dossiers |
| HOLD | Respect existing hotel freezes / write restrictions | Any frozen hotel |

## Binding constraints

- No unauthorized Findymail spend beyond per-hotel credit hard-caps.
- No Surfe auto.
- No CURRENT promotion.
- No new Airtable bases/tables without demonstrated need + authorization.
