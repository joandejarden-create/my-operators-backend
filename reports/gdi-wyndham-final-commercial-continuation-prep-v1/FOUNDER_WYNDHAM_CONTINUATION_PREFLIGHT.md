# FOUNDER PREFLIGHT — Wyndham Final Commercial Continuation ($0)

**At:** 2026-11-27  
**Hotel:** `recnPB3Q02NO8Ma1o`  
**Baseline:** Accepted $0 commercial-quality review — **26 proposed** qualified  
**PR capability:** [#613](https://github.com/joandejarden-create/my-operators-backend/pull/613) Full E2E V3 WHO/HOW  
**STOP:** No paid research · No Airtable · No STAGED · No CURRENT · No Findymail/Surfe

---

## Accepted write-set baseline (do not apply yet)

| Bucket | Count | Notes |
|---|---:|---|
| CLEAN 11 | 11 | Frozen preserve |
| Turner Construction | 1 | Prior cert FQO |
| ACG San Diego | 1 | Prior validated |
| New QA write-ready | 13 | After Saronic merge; Bora both retained |
| **Total proposed** | **26** | |
| HOLD | 2 | Arevon; Level 3 |

Dedup preserved: Bora ×2 distinct; Saronic consolidated; Turner+Flatiron cross-linked.

Source: `reports/gdi-wyndham-final-commercial-completion-v1/`

---

## Prerequisites for paid WHO/HOW / leisure / PLP

| Prerequisite | Status |
|---|---|
| WHO/HOW Agent contract integrated | READY in PR #613 (+ this push: mandatory `whyWeAreShowingThisContact`) |
| Alternative-entry-point schema validated | READY — fields + generic-why rejection |
| Mandatory contact rationale enforced | READY — entry points fail without opportunity-specific why |
| Contact research stage connected | READY — Stage 4 DryRun |
| PowerShell runners tested | READY — see § PowerShell evidence |
| Exa job-store dedupe preserved | READY — existing store; Stage 3 must reuse settled jobs |
| Budget ledger operational | READY — existing paid ledger; new auth separate |
| Integration Controller merge complete | **BLOCKED** — PR #613 OPEN |

**Do not run paid WHO/HOW until #613 is merged and `ws-gdi` is home-synced.**

---

## Proposed paid authorization (awaiting founder GO after merge)

```
FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127
```

| Item | Value |
|---|---|
| Hard ceiling | **USD 18.00** |
| Expected spend | ~USD 12.50 |
| Prior unused $7.90 | **NOT carried** |
| Findymail | OFF |
| Surfe | OFF (explicit-click only) |
| Airtable / STAGED / CURRENT | OFF |

### Priority order

1. PLP recovery (rewritten referral queries; evidence-gated)  
2. Live leisure ×6 applicable modules (destination/events/sports first)  
3. Deduped Agent deep for leisure discoveries  
4. Competitive/overflow CAD on 15 association/event HOLDs  
5. WHO/HOW Agent on 25 priority continuations (+ 2 entry-point fallback)  
6. Public-source contact discovery  
7. Strict FQO reassessment  

### Hard stop rules

- Ceiling hit → PARTIAL, not false COMPLETE  
- No rediscovery of original 161  
- No re-bill settled Exa job IDs  
- No Findymail/Surfe/Airtable/STAGED/CURRENT  
- Bethesda + CLEAN11 freezes ON  

---

## PowerShell operational evidence (re-verified this pass)

Commands executed under DryRun (exit 0):

```powershell
.\scripts\gdi\Invoke-GdiDiscovery.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiDeepResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -ContinuationOnly -Mode DryRun
.\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiQualification.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
```

Unified validation: `Invoke-GdiValidateFullE2E.ps1`  
npm: `npm run test:gdi-canonical-full-e2e-v3-v1`

Live Mode remains fail-closed in V3 CLI until authorized Exa adapter tranche after merge.

---

## Post-research report (Part G) — deferred

Items 1–15 of the founder post-research report will be produced **after** paid GO + execution. This preflight intentionally stops before spend.

---

## Status

**`$0` IMPLEMENTATION + PREFLIGHT COMPLETE**  
**AWAITING_INTEGRATION_CONTROLLER** (PR #613)  
**THEN** founder paid GO `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127`
