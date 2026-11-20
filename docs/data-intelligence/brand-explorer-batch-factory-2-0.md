# Brand Explorer Batch Factory 2.0

**Contract:** `BE_FACTORY_2_0_BATCH_V1`  
**Status:** Staging + portfolio review enabled. Production Presentation writeback remains founder-gated per exact candidate.

## Purpose

Stop per-brand founder interruption during research. Queue all Active/Live Brand Basics brands, research/stage/certify automatically, then review the portfolio together in Admin Resources → **Brand Explorer Batch Review**.

## Hard rules

- Curio (`receQkxgjlezsc1xg`) and Ascend (`reclkgOzvAcBheUSo`) are **SKIPPED_PROTECTED** — no reprocessing.
- Autograph Factory 1.0 candidate is preserved and processed first.
- No generic approval token — each brand requires its exact `candidateHash`.
- Approval records tokens + write-plan fingerprints only; **does not** write production Presentation.
- Paid research budget defaults to `$0` until explicitly raised per brand.
- Missing-brand creation is out of scope until the existing inventory is processed.

## Commands

```bash
npm run be-factory-2-queue-build          # Airtable Active/Live → durable queue
npm run be-factory-2-process-batch -- --limit 5
npm run be-factory-2-process-batch -- --force --brand rec…
npm run be-factory-2-confirm-batch-ids
npm run be-factory-2-named-batch-five     # AC / Moxy / Vignette / Canopy / EVEN depth batch
npm run be-factory-2-batch3               # voco / Motto / avid / Tribute / Indigo
npm run be-factory-2-batch4               # Tapestry / Tempo / Design / Aloft / Kimpton / Bunkhouse / Centric / Courtyard / Westin / HI Express
npm run be-factory-2-batch5               # JW / W / Le Méridien / Waldorf / Conrad / LXR / Park Hyatt / Grand Hyatt / Andaz / Hyatt Place
npm run be-factory-2-batch6               # Ritz / St. Regis / EDITION / Luxury Collection / Renaissance / Signia / Alila / Hyatt House / InterContinental / Six Senses
npm run be-factory-2-batch7               # Garner / Sleep Inn / Econo / Rodeway / MainStay / Days Inn / Super 8 / Microtel / Baymont / Travelodge
npm run be-factory-2-dashboard
npm run test:be-factory-2-batch-v1
npm run test:be-factory-2-named-batch-five-v1
npm run test:be-factory-2-batch3-v1
npm run test:be-factory-2-batch4-v1
npm run test:be-factory-2-batch5-v1
npm run test:be-factory-2-batch6-v1
npm run test:be-factory-2-batch7-v1
```

## Named batch five (2026-11-20)

Brands: AC Hotels, Moxy, Vignette, Canopy, EVEN.  
Identities confirmed against live Brand Basics. Depth candidates staged with exact-six loyalty cards and Commercial Engine gap-fill.  
**Disposition:** `NEEDS_REMEDIATION` until verified fee exhibits are attached (paid budget still $0).  
Autograph remains `READY_FOR_REVIEW`; Curio/Ascend remain protected.  
Review package: `reports/brand-explorer/factory-2-0/13-named-batch-five-consolidated-review.md`.

## Batch 3 (2026-11-20)

Brands: voco Hotels, Motto by Hilton, avid hotels, Tribute Portfolio, Hotel Indigo.  
Same depth factory; fee exhibits remain the READY blocker ($0 paid).  
Review package: `reports/brand-explorer/factory-2-0/21-batch3-consolidated-review.md`.

## Batch 4 (2026-11-20)

Ten brands: Tapestry, Tempo, Design Hotels, Aloft, Kimpton, Bunkhouse, Hyatt Centric, Courtyard, Westin, Holiday Inn Express.  
All staged `NEEDS_REMEDIATION` pending verified fee exhibits ($0 paid).  
Review package: `reports/brand-explorer/factory-2-0/31-batch4-consolidated-review.md`.

## Batch 5 (2026-11-20)

Ten brands previously absent from Active/Live Factory inventory but **already present** as Draft/Under Review Brand Basics (no new Basics creates): JW Marriott, W Hotels, Le Méridien, Waldorf Astoria, Conrad, LXR, Park Hyatt, Grand Hyatt, Andaz, Hyatt Place.  
Empty Presentation → staged create plans; fee exhibits + verified gallery CDN remain READY blockers ($0 paid).  
Review package: `reports/brand-explorer/factory-2-0/41-batch5-consolidated-review.md`.

## Batch 6 (2026-11-20)

Ten brands: Ritz-Carlton, St. Regis, EDITION, Luxury Collection, Renaissance, Signia by Hilton, Alila, Hyatt House, InterContinental, Six Senses.  
All EXISTING_CANONICAL Draft/Under Review (no Basics creates). Management-led luxury fee language does not invent franchise percentages.  
Review package: `reports/brand-explorer/factory-2-0/51-batch6-consolidated-review.md`.

## Batch 7 (2026-11-20)

Economy / midscale / affordable extended-stay: Garner, Sleep Inn, Econo Lodge, Rodeway Inn, MainStay Suites, Days Inn, Super 8, Microtel, Baymont, Travelodge.  
Choice Privileges + Wyndham Rewards loyalty-six; FDD disclosure language only (no invented fees).  
Review package: `reports/brand-explorer/factory-2-0/61-batch7-consolidated-review.md`.

## Durable storage

- `data/brand-explorer/factory-2-0/queue.json`
- `data/brand-explorer/factory-2-0/brands/<brandId>/stage.json` (+ candidate, certification, write-plan, spend)
- `data/brand-explorer/factory-2-0/approvals/`
- `data/brand-explorer/factory-2-0/portfolio-dashboard.json`
- Mirrored reports under `reports/brand-explorer/factory-2-0/`

## Admin UI

- Route: `/app#/brand-explorer-batch-review`
- API: `/api/brand-explorer-batch-review/portfolio` (+ brand, queue, approve)
- Auth: internal runbook admin

## Status vocabulary

`QUEUED` · `RESEARCH_IN_PROGRESS` · `READY_FOR_REVIEW` · `NEEDS_REMEDIATION` · `APPROVED` · `PUBLISHED` · `SKIPPED_PROTECTED` · `BLOCKED`

CURRENT Presentation baselines are staged as `NEEDS_REMEDIATION` until a Factory-depth candidate closes gaps. Only Autograph (exact candidate + permanent standards) and future Curio-parity candidates reach `READY_FOR_REVIEW`.
