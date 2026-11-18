# Phase 2A — Existing Full E2E workflow map

**Authority:** `lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js` (`PIPELINE_STAGES` S0–S12)  
**OS wrapper:** `lib/dealality-full-e2e-os/adp-full-e2e-workflow-v1.js`  
**Contract doc:** `reports/dealality-adp-permanent-full-e2e-certification-v1/ADP_PERMANENT_FULL_E2E_CERTIFICATION_CONTRACT.md`

Do not rebuild working stages. Hotel Explorer progressive enrichment is a separate workstream.

## Pipeline

| Stage | Entry / modules | Inputs | Outputs / storage | Gates | Auth / controls | Known gap |
|-------|-----------------|--------|-------------------|-------|-----------------|-----------|
| **S0 Identity** | `dealality-full-e2e-os/hotel-identity.js`, `adp-gdi-canonical-identity.js`, `data/hotel-identity/canonical-hotel-identity-ledger-v1.json`, `fixtures/.../census-links-v1.json` | `adpPropertyId`, HPC `rec…` | Ledger + run manifest under `data/ai-demand-positioning/autonomous-full-e2e-runs/` | `test:adp-canonical-31-scope-v1` | Ambiguity → identity approval | Bethesda `COMPATIBILITY_ONLY`; dual-source shadow local-only |
| **S1 Research** | Orchestrator + HPC onboarding / research ledgers | Census/HPC evidence | Staged research artifacts | `audit:adp-property` | No paid without later auth | Live Airtable HE smoke often BLOCKED_ENV |
| **S2 HPC/ADP persist** | HPC create/reuse modules | Validated identity | Airtable HPC/ADP rows | Collision checks | **Explicit write approval** | Default deny |
| **S3 Attributes** | `property-attributes/*`, Gate 1 dedup | HPC + HI evidence | Certified attribute set | Dedup G1 | Before paid monitoring | Taxonomy unify → Phase 2B |
| **S4 Peer governance** | Brand portfolio peer set | Peer membership | Frozen peer intent | BPP completeness planners | Founder when changing peers | Many hotels `PEER_SET_NOT_FROZEN` |
| **S5 Cost auth** | `adp-paid-supervisor-handoff-v1.js` | Cost contract | Auth ledger | Hard gate | **Required before paid** | — |
| **S6–S7 Monitoring** | Multi-provider runner, scenario registry | Authorized prompts | Runtime periods `data/ai-demand-positioning/runtime/` | Prompt integrity / scenario parity | Paid + approval | Not run in Phase 2A |
| **Visibility** | Presence index / provider presence | Raw responses | Customer presence tables | Measurement assurance gates | Runtime `mentioned` non-authoritative alone | — |
| **S8 Analytics** | Competitor entity resolution, displacement, RG | Observations + attributes | Rank / RG / lost-demand | Dedup G2; displacement parity | Fail-closed without registry | Displacement exact parity FAIL (smoke + packs) |
| **S8 Reconciliation** | Permanent full-e2e certification modules | Original response hashes | `METRICS_RECONCILED` | `test:adp-permanent-full-e2e-certification-v1` | Fail closed before report | Layer B durable missing |
| **S9 Report** | Owner payload, executive read v3, staged store | Certified period | Staged JSON | Executive / customer-surface | Not auto-publish | Territory rates / evidence / BPP FAIL |
| **S9B History** | Longitudinal persist + Airtable ecosystem contract | Staged payload | History tables / FS archives | `test:adp-e2e-persistence-contract-v1` | History writes off by default | Layer B measured-dates missing |
| **S10 PDF/share** | Share / PDF generators | Staged report | Test pins only | Share production contract | No production tokens by default | — |
| **S11 Release audit** | 31-hotel customer release acceptance | Staged + artifacts | QA result | Dedup G3 | Blocks CUSTOMER_READY on dupes | — |
| **S12 Founder** | Lifecycle state machine + production release workflow R0–R8 | Staged + PDF SHA | `AWAITING_PUBLICATION_APPROVAL` | Admin workflow tests | **Publication separate auth** | No single npm Full E2E CLI alias yet |
| **Publication** | `published-snapshot.js`, `data/ai-demand-positioning/published/`, `api/ai-demand-positioning.js`, `public/owner-ai-demand.html` | Approved package | Live FS manifests + owner UI | Customer-surface | Memberstack / share tokens | 33 surface failures block claim |
| **Recurring** | Monthly review eligibility + builders | Prior periods | `reports/ai-demand-positioning/monthly-review/` | Longitudinal / recert assurance | Paid refresh = REFRESH mode | Automation gated |

## Primary npm reliability gates

```bash
npm run test:adp-canonical-31-scope-v1
npm run test:adp-canonical-31-certification-posture-v1
npm run test:adp-permanent-full-e2e-certification-v1          # Layer A; Layer B optional/required via env
npm run test:adp-e2e-persistence-contract-v1
npm run test:adp-customer-surface-contract
npm run test:adp-phase1-reliability-gates-documented         # chains persistence + permanent + surface
```

## Doctrines preserved

- Publication ≠ certification  
- Offline Layer A PASS ≠ production proof  
- Offline tests alone must not auto-approve customer-facing results  
- Fail closed on missing evidence (no invention)
