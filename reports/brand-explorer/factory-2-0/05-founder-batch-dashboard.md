# Factory 2.0 — Consolidated Portfolio Dashboard

**Contract:** `BE_FACTORY_2_0_MASTER_QUEUE_RECONCILE_V1`
**Generated:** 2026-11-20T06:45:16.033Z

## Reconciliation

| Metric | Count |
|---|---:|
| Authorized batches | 7 |
| Authorized brand slots | 51 |
| Prior Active/Live queue | 98 |
| Staged brand folders | 58 |
| **Master queue total** | **98** |

## Portfolio status

| State | Count |
|---|---:|
| Total brands in master queue | 98 |
| Existing brands still QUEUED | 38 |
| EXISTING_CANONICAL in queue | 30 |
| New brands proposed | 0 |
| Researching | 0 |
| Certified (ready for review) | 1 |
| Awaiting approval | 1 |
| Needs remediation | 57 |
| Persisted verified | 0 |
| Published | 0 |
| Blocked | 0 |
| Protected (skip) | 2 |
| Research spend USD | 0 |
| Expected publication writes | 5362 |

## Authorized batches accounted for

- **FACTORY_1_AUTOGRAPH** (EXISTING_ACTIVE) — 1 brands — Factory 1.0 Autograph (preserved)
- **NAMED_BATCH_FIVE** (EXISTING_ACTIVE) — 5 brands — Named batch five (AC / Moxy / Vignette / Canopy / EVEN)
- **BATCH_3** (EXISTING_ACTIVE) — 5 brands — Batch 3 (voco / Motto / avid / Tribute / Indigo)
- **BATCH_4** (EXISTING_ACTIVE) — 10 brands — Batch 4 (10 Active/Live depth brands)
- **BATCH_5** (EXISTING_CANONICAL) — 10 brands — Batch 5 (luxury/upscale EXISTING_CANONICAL Draft/Under Review)
- **BATCH_6** (EXISTING_CANONICAL) — 10 brands — Batch 6 (luxury / soft-brand EXISTING_CANONICAL)
- **BATCH_7** (EXISTING_CANONICAL) — 10 brands — Batch 7 (economy / midscale EXISTING_CANONICAL)

## Next 10 scheduled

| # | Brand | Status | Batch |
|---:|---|---|---|
| 1 | Autograph Collection (`recEJCTDj1zrsjPM6`) | CERTIFIED_READY_FOR_REVIEW | FACTORY_1_AUTOGRAPH |
| 2 | avid hotels (`recoEarnE8T6sDjZq`) | NEEDS_REMEDIATION | BATCH_3 |
| 3 | Hotel Indigo (`recegXrqaPiSLGCIe`) | NEEDS_REMEDIATION | BATCH_3 |
| 4 | Motto by Hilton (`reclt44apoi8co0e6`) | NEEDS_REMEDIATION | BATCH_3 |
| 5 | Tribute Portfolio (`recCvV0PuZOi8c3hC`) | NEEDS_REMEDIATION | BATCH_3 |
| 6 | voco Hotels (`recwONQTqGU1jHCsM`) | NEEDS_REMEDIATION | BATCH_3 |
| 7 | Aloft Hotels (`recJ1GZQpttX7qHgw`) | NEEDS_REMEDIATION | BATCH_4 |
| 8 | Bunkhouse Hotels (`recGv268Wda31PlSZ`) | NEEDS_REMEDIATION | BATCH_4 |
| 9 | Courtyard by Marriott (`rec6hye5H8zJmAGv3`) | NEEDS_REMEDIATION | BATCH_4 |
| 10 | Design Hotels (`rec02zPClpWUTCyXM`) | NEEDS_REMEDIATION | BATCH_4 |

## Gates preserved

- Exact-candidate approval required for writeback authorization
- No production Presentation overwrite without founder exact-hash approval
- No new Brand Basics creates without identity + creation approval
- Paid research budget remains $0 unless explicitly raised
- Curio / Ascend remain SKIPPED_PROTECTED
