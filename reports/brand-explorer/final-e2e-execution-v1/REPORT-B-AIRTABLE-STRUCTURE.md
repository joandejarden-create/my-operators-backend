# Report B — Airtable Structure & Persistence

## Canonical ownership (existing — not invented)

| Domain | Base env | Table |
|--------|----------|-------|
| Brand identity | `AIRTABLE_BASE_ID` | Brand Setup - Brand Basics |
| Customer presentation | `AIRTABLE_BASE_ID` | Brand Setup - Brand Explorer Presentation |
| Brand Setup children | `AIRTABLE_BASE_ID` | Footprint, Project Fit, Portfolio & Performance, Standards, Fees, Deal Terms, Ops Support, Legal, Loyalty & Commercial, ESG |
| HPC footprint | `AIRTABLE_BASE_ID_ALT` | Hotel Property Census |
| Census (transitional) | `AIRTABLE_BASE_ID_ALT` | Hotel Census |
| Aliases | `AIRTABLE_BASE_ID_ALT` | Brand Alias Mapping |

Contract: `lib/brand-explorer/be-airtable-integration-sot-v1.js`  
Activation: `configs/brand-explorer-founder-publication-activation-v1.json` (`productionWritesAuthorized: false`)

## Field ownership principle

- **Presentation table** — founder-publish customer-facing copy (slotKey / Title / Body / Sort / Active / images)
- **Brand Basics / Setup children** — structured characteristics; factory stages only where write class permits
- **HPC** — geographic / rooms / Open|Pipeline authority; never overwritten by company-reported marketing counts
- **Research evidence** — retain provenance separately; never collapse into a single text blob as sole SoT

## Curio / Ascend write results

| Brand | Dry-run status | Airtable writes | Notes |
|-------|----------------|-----------------|-------|
| Curio `receQkxgjlezsc1xg` | `BLOCKED_GATES` | **0** | Gate1/Gate2/scorecard not FOUNDER_APPROVED |
| Ascend `reclkgOzvAcBheUSo` | `BLOCKED_GATES` | **0** | Same + **priorityBlocked=1** |

Content hashes (latest dry-run):

- Curio proposed: see `publication-runs/receQkxgjlezsc1xg-BLOCKED_GATES-2026-11-19T22-21-07-351Z.json`
- Ascend proposed: `b90ae03e42ec071ed6a7a7f5d801cd46d8eaf843b551515166d43f8b02523235`

## Required before any apply

1. Formal Gate 1 founder approval artifact  
2. Gate 2 unblocked + approved  
3. Scorecards → `FOUNDER_APPROVED`  
4. Ascend priority gate cleared  
5. Write plan bound to **existing Presentation record IDs** (updates), not 214 blind creates  
6. Immutable backup + readback verify  
7. Flip activation + `BE_FOUNDER_PUBLICATION_PRODUCTION_WRITES=1` only after above  
8. Staged publication boundary — never expose partial profile as customer-live  

## Schema additions

None applied in this cycle. Extensions only after validating non-duplication against Brand Setup / HPC / Source Library.

## E2E classification

**PERSISTENCE_DESIGNED** — SoT + publication workflow coded; **not** PERSISTENCE_VALIDATED / CUSTOMER_PUBLISHED.
