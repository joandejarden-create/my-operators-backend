# SEC EDGAR V2.2 — Delivery Report

**PR:** #492  
**Generated:** 2026-11-20  
**Paid research spend:** $0  
**Apply / merge / new Airtable schema:** NOT authorized (dry-run + proposal only)

## Live evidence (DiamondRock)

| Metric | Result |
|---|---|
| Filings reviewed | 3× 10-K (2023–2025 YE) + 1× 8-K EX-10 package |
| Unique VERIFIED_PROPERTY hotels | **38** (deduped; 90 schedule rows across years) |
| Measured precision (verified sample) | **100%** (38/38) — gate ≥95% **PASS** |
| Approx brand-family recall | **71%** (5/7 expected brand hints) |
| Property-level franchise expiration years in these 10-Ks | **0** (portfolio tables list Hotel/City/State/Manager — not expiration years) |
| EX-21 subsidiaries processed | 3 (distinct source URLs, parent accession preserved) |
| EX-10 processed | 1 (`exhibit1016324.htm` under 8-K parent) |
| Explicit LEGAL_OWNER extractions (tightened) | 0 false “brand owned by …” hits |
| Manager disclosures (schedule Manager column) | 105 row-level HISTORICALLY_VERIFIED manager attributions |
| HPC enrichment | dry-run only — no `--apply` |

### Demonstrated

1. **Hotel identification** — real DiamondRock portfolio HTML tables → city/state-backed VERIFIED_PROPERTY  
2. **Contract dates** — fixture + text schedule extractor retained; these live 10-Ks do not publish property-level franchise expiration years in the portfolio table (honest gap)  
3. **Owner/operator** — Manager column → MANAGEMENT_COMPANY; registrant ≠ owner; ownership requires fee-simple/legal-owner language  
4. **Historical vs current** — schedule managers marked HISTORICALLY_VERIFIED from filing date  
5. **Duplicate elimination** — 90 rows → 38 unique identities  
6. **Source evidence** — per-exhibit URLs + parent accession  
7. **HPC matching** — reconciliation dry-run path only  
8. **Unmatched retention** — filesystem inventory + Airtable schema **proposal** (not created)

## Coverage ledger reconciliation

| Cohort | Eligible | Checkpoint processed | Notes |
|---|---|---|---|
| CORE_SIX | **3,863** | **20** (DiamondRock) | Marriott/Hilton/Sunstone/Host/Ashford eligible not yet checkpointed |
| EXTENDED | eligible inventory not built | **30** (Apple 15 + RLJ 15) | Do **not** merge into 3,863 |
| Checkpoint total | — | **50** | 20 + 30; not double-counted |

Exhibits enumerated ≠ searched: only processed exhibits are claimed searched.

## Property identity quality

- Raw extraction evidence retained  
- Identity classes: VERIFIED / PROBABLE / AMBIGUOUS / MULTIPLE / GENERIC / ARTIFACT / DUPLICATE_ALIAS  
- Uncertain hotels are **never** silently promoted to VERIFIED_PROPERTY  
- Footnote markers like `(2)` stripped from display names  

## Permanent inventory schema

See `docs/ai-build-system/SEC_EDGAR_HOTEL_DISCOVERY_AIRTABLE_SCHEMA_PROPOSAL.md` — **founder approval required** before Meta API create.

## Remaining research queue (priority)

1. Property-level franchise / management expiration schedules (older exhibits / supplemental tables)  
2. Hotel-specific EX-10 HMA / franchise agreements (content-filter employment/credit EX-10s)  
3. EX-21 SPE → property ownership linking  
4. Acquisition/disposition exhibits  
5. Broader CORE_SIX checkpoint expansion under fair-access caps  

## Governance

- No merge  
- No customer publish  
- No broad `--apply`  
- HE concurrent dirty work preserved  
- CI: `npm run test:sec-edgar-v22-exhibits-relationships` + identity-quality + live evidence eval  
