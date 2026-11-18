# Implementation Map — GDI Discovery Intelligence Full E2E Enhancements V1

**Base:** `main` @ `4d3b3cb61` (includes PR #345–#349)  
**Paid providers / Airtable production / publication:** not authorized in this program.

## Existing (extend — do not duplicate)

| Capability | Module | Status |
|---|---|---|
| Canonical Full E2E entry | `lib/.../workflow/gdi-canonical-full-e2e-entry-v1.js` | Keep |
| Original research chain + `maxCandidates` slice | `lib/.../workflow/original-full-e2e-research-chain-v1.js` | Extend (screen-before-cap in PR2; selection register in PR3) |
| Fourteen-segment discovery | `lib/.../workflow/full-e2e-fourteen-segment-discovery-execution-v1.js` | Keep; mission sharpening in PR5 |
| Fair hotel×segment scheduler | `lib/.../workflow/gdi-segment-hotel-fair-research-scheduler-v1.js` | Extend scoring weights in PR3 |
| Low-cost screen + investment score | `lib/.../research/commercial-pursuit-strategy-v1.js` | Extend outcomes (global-only / enrichment) in PR2 |
| Depth tiers T1/T2/T3 | `lib/.../research/deep-research-eligibility-v1.js` | Keep; wire selection visibility in PR3 |
| Pre-paid deep org dedupe | `lib/.../research/gdi-paid-deep-two-stage-dedupe-v1.js` | Strengthen with **published package** inventory in PR2 |
| Hotel-scoped prior reuse | `lib/.../workflow/reuse-prior-intelligence-v1.js` | Keep hotel-scoped; do not put private feedback in global |
| Org / edition identity keys | `normalizeOrgIdentityKey`, `editionIdentityKey` | Reuse for global IDs |
| Geography OUT_OF_MARKET fit | `gdi-commercial-hypothesis-development-v1.js` | Hotel rejection exists; **global retention missing** |
| Execution observability | `lib/.../observability/*` | Add research-selection rows in PR3 |
| PowerShell hunters | `scripts/gdi-nextgen-hunters/*.ps1`, `scripts/gdi-workflow.ps1` | Pattern for thin track wrappers (PR4) |
| Airtable dry-run proposals | e.g. `need-period-airtable-schema-proposal-v1.js` | Pattern for PR1 Airtable activation plan |
| Bethesda immutable reference | `reports/gdi-bethesda-exa-live-discovery-effectiveness-v1/` | Replay fixture for PR2–PR3 |

## Gaps (this program)

1. **No cross-hotel global discovered-intelligence store** — out-of-market evidence discarded as hotel reject.
2. **Screening not applied to all raw candidates before `maxCandidates`** — fair queue then slice.
3. **Published CUSTOMER_PUBLISHED package not authoritative pre-deep dedupe source** — local `opportunities.json` / ORS only (86 vs 67 risk).
4. **No research-selection register** answering why #20 vs #21 got deep spend.
5. **No first-class social / planner / venue mission packs** on existing segments.
6. **Exa missions** still too national-calendar for Bethesda catchment (live evidence).
7. **Tenant isolation tests** for any cross-hotel public reuse.

## PR boundaries

| PR | Scope | Primary modules |
|---|---|---|
| **PR1** | Global intelligence identity + local store + privacy + Airtable dry-run map + chain hook for out-of-market retention | `lib/.../global-intelligence/*`, chain persist hook, tests, `AIRTABLE_ACTIVATION_PLAN` draft |
| **PR2** | Screen-all-candidates before cap; published-package dedupe; screen decision ledger | pursuit screen extensions, dedupe, chain collect path |
| **PR3** | Two-pass selection; transparent ranking; selection observability; Bethesda 165 replay | scheduler + eligibility + observability |
| **PR4** | Three local tracks as mission packs + thin PS1 wrappers | mission configs + wrappers → canonical entry |
| **PR5** | Fourteen-segment Exa mission / geo targeting | Exa contracts + hotel catchment context |
| **PR6** | Full E2E fixture certification + program reports + privacy regression | reports + integration tests |
| **PR7 (this)** | Mandatory Airtable persistence contract against **existing** ADP tables + Bethesda reconciliation + dry-run read-back gate | `gdi-full-e2e-airtable-persistence-*`, Bethesda recon, activation plan |

## Non-goals

- New orchestrator / parallel GDI architecture
- Paid Exa / Apify / enrichment
- Production Airtable writes (dry-run / recon only until authorized)
- Customer publication / CURRENT pointer changes
- Customer-facing drawer UX
- Creating competing “global demand” Airtable tables when Demand Generators / Opportunities / PE / Research tables already exist
