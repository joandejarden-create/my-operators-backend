# GDI_FULL_E2E_RESEARCH_FOUNDATION_READY

**Worktree:** `C:\Dev\dealality\my-operators-backend-gdi-research-from-main`  
**Branch:** `research/gdi-continue-from-main-v1`  
**Base:** `520884d0f73520fb6193525f582df328d804b320`

## 1. Research improvements recovered (classification)

| Source | Classification | Notes |
| --- | --- | --- |
| `feat/gdi-full-e2e-contract` (v1) | **SUPERSEDED** | Main already has Full E2E **v1.5** |
| `fix/gdi-full-e2e-renaissance-lessons` (v1.2) | **SUPERSEDED** | Lessons + publication-quality already on main v1.5 |
| `gdi/lmc-planner-contact-depth-scoped` | **SUPERSEDED** (mostly) | Contact-depth / org-fallback already on main buyer enrichment |
| Spice dirty `full-e2e-contract` lesson deltas | **SUPERSEDED** | `e2d_*`, sales-packet, city-scope lessons already on main |
| `hotel-execution-scope-preflight-v1` + wiring | **READY_TO_INTEGRATE** → **integrated** | Was referenced by main lessons but module/wiring missing |
| Event-to-demand missing modules (buyer resolution, hotel fit, discovery, …) | **READY_TO_INTEGRATE** → **integrated** | Completes E2D pack already partially on main |
| Need-date core (priorities, profile store, orchestrator, adaptive, …) | **READY_TO_INTEGRATE** → **integrated** | Portal auth / live option-B runners left **EXPERIMENTAL** |
| Unified discovery option-B live runners / Waterstone live campaign | **EXPERIMENTAL** | Not integrated; preserve on spice worktree |
| Dirty `gdi-research-to-page-workflow-v1.mjs` wholesale | **NEEDS_REMEDIATION** | Only scope-gate slices integrated; full dirty diff preserved on spice branch |

## 2. Research improvements integrated

- Hotel execution-scope preflight + coordinator gates (`validate_config`, `paid_multisegment`)
- Event-to-demand completion modules + existing sales-intelligence packet (already on main)
- Need-date commercial priorities / profile / orchestrator core
- Opportunity research-quality evaluator (demand class, qualification lane, completion, DOS lens)
- Three-hotel dry-run harness
- Offline Spice work-inventory fixtures for portfolio parity
- Tampa identity-separation verification fixture (auth gate)

## 3. Remaining gaps

- Spice: **0 named/verified contacts** on local inventory; 17 duplicate titles; volume ≫ 10–15 quality band
- Bethesda/Waterstone: credible prospects still above 10–15 band after honest reclassification — needs editorial cull + contact depth, not more volume
- Need-date deep-discovery v6/v7 and portal auth remain experimental
- Full dirty coordinator + Nobu E2D disposition scripts still on spice worktree pending focused PRs
- Production still on pre-consolidation Railway image (out of scope)

## 4. Three-hotel dry-run (local inventory, no paid)

See `00-three-hotel-research-quality.md`.

| Hotel | Total | Sales Ready | Account Dev | Needs Research | Credible (SR+AD) | Named contacts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Spice Island | 77 | 0 | 35 | 42 | 35 | 0 |
| Bethesda Marriott | 54 | 19 | 19 | 16 | 38 | 29 |
| Waterstone | 29 | 11 | 6 | 12 | 17 | 22 |

## 5. Contact & demand-thesis quality

- Policy preserved: **reliable inferred demand may qualify without RFP**
- Stale `demandStatus=CONFIRMED` labels no longer auto-promote to CONFIRMED_DEMAND
- Spice contact gap is the primary research blocker for Sales Ready
- Bethesda strongest named-contact coverage among the three

## 6. Research completion standard

`gdi-opportunity-research-quality-v1` + existing Full E2E v1.5 completion evaluator:

- Commercially sufficient ≠ title+company+contact alone
- Requires hotel-specific thesis, evidence, and buyer/entry point
- Exhausted incomplete distinguished from unfinished assessment
- Bounded budgets + city-cannot-select-property paid gate wired

## 7. Proposed next pilot

**Bounded multi-hotel Full E2E research pilot — contact depth + quality cull**

| Item | Proposal |
| --- | --- |
| Hotels | Spice Island, Bethesda Marriott, Waterstone (same three) |
| Categories | Association, corporate meetings/training, incentive, intermediaries, scaling (hotel-fit only) |
| Baseline | Spice 77 / Beth 54 / Water 29 local opps; SR+AD 35 / 38 / 17 |
| Gaps | Spice named buyers; Spice dedupe; cull to ~10–15 credible/hotel; need-date ranking where profiles exist |
| Strategy | Reuse prior intelligence → buyer-contact enrichment (Surfe off by default) → sales-packet synthesis → Full E2E evaluate_completion dry-run |
| Expected value | Sales-ready shortlists DOS can action; remove duplicate/noise volume |
| Max paid budget | **$36** ($12/hotel × 3) under existing portfolio auth pattern; **$0** until founder approves |
| Success metrics | ≥10 and ≤15 credible/hotel; Spice named contacts > 0 on top 15; duplicates < 5/hotel; Full E2E PARTIAL→clear gap list |
| Stop | Budget hit, three consecutive empty buyer searches/account, or founder hold |

## 8. Maximum proposed research spend

**$36 USD** total for the three-hotel contact-depth pilot (optional). Default recommendation: **$0 dry-run first**.

## 9. Exact next decision required

**Approve or reject** the $0 dry-run contact-depth + quality-cull pilot on the three hotels (no Airtable/publish/Railway).  
Separately: approve paid envelope only if dry-run gap list shows named-buyer research is blocked without Surfe/Parallel.
