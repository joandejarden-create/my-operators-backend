# FOUNDER PREFLIGHT — Wyndham Final Commercial Continuation ($0)

**At:** 2026-11-27  
**Hotel:** `recnPB3Q02NO8Ma1o`  
**Baseline:** Locked **26** proposed qualified (not applied)  
**PR capability:** [#613](https://github.com/joandejarden-create/my-operators-backend/pull/613) Full E2E V3 + live WHO/HOW adapter  
**STOP:** No paid research · No Airtable · No STAGED · No CURRENT · No Findymail/Surfe

---

## Accepted write-set baseline (LOCKED — do not apply)

| Bucket | Count | Notes |
|---|---:|---|
| CLEAN 11 | 11 | Frozen preserve |
| Turner Construction | 1 | Prior cert FQO |
| ACG San Diego | 1 | Prior validated |
| New QA write-ready | 13 | After Saronic merge; Bora both retained |
| **Total proposed** | **26** | |
| HOLD | 2 | Arevon; Level 3 |

Dedup preserved: Bora ×2 distinct; Saronic consolidated; Turner+Flatiron cross-linked.

Lock artifact: `10-LOCKED_26_COMMERCIAL_BASELINE.json`  
Source: `reports/gdi-wyndham-final-commercial-completion-v1/`

---

## Prerequisites for paid WHO/HOW / leisure / PLP

| Prerequisite | Status |
|---|---|
| WHO/HOW Agent contract integrated | READY in PR #613 |
| Live Exa WHO/HOW adapter (product-wide) | READY — `gdi-exa-who-how-live-adapter-v1.js` |
| Alternative-entry-point schema validated | READY |
| Mandatory contact rationale enforced | READY |
| Contact research stage connected | READY — Stage 4 DryRun |
| PowerShell runners tested | READY — five stages + Full + Validate |
| Exa job-store dedupe / resume | READY — $0 validated |
| Budget ledger / hard ceiling | READY — $0 validated |
| Integration Controller merge complete | **BLOCKED** — PR #613 OPEN |
| GDI post-merge home sync | **BLOCKED** until merge |
| Explicit founder paid GO | **BLOCKED** |

**Merging #613 alone does not enable paid spend.** V3 CLI Live remains fail-closed; paid Checkpoint A uses the live adapter only after founder GO.

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
| Checkpoint A WHO/HOW | **USD 5.00 max** (inside $18 — no separate $5 auth) |
| Findymail | OFF |
| Surfe | OFF (explicit-click only) |
| Airtable / STAGED / CURRENT | OFF |

Exact manifest: `11-PAID_EXECUTION_MANIFEST_18USD.json`  
Readiness: `12-READINESS_CONFIRMATION.md`

### Updated priority order (after founder GO)

**Checkpoint A first**

1. WHO/HOW Agent on top 25 existing continuations (max $5)  
2. Checkpoint report → founder acceptance before releasing remainder  

**Checkpoint B (remaining aggregate budget)**

1. PLP recovery (rewritten evidence-gated referral queries)  
2. Competitive/overflow CAD on 15 association/event HOLDs  
3. Six applicable live leisure modules  
4. Deduped deep research on genuinely new leisure discoveries  
5. Public-source professional contact discovery  
6. Strict FQO reassessment vs locked 26  

### Hard stop rules

- Ceiling hit → PARTIAL, not false COMPLETE  
- No rediscovery of original 161  
- No re-bill settled Exa job IDs  
- No Findymail/Surfe/Airtable/STAGED/CURRENT  
- Bethesda + CLEAN11 freezes ON  
- Recovery &lt;10/25 after Checkpoint A → stop for founder direction  

---

## PowerShell operational evidence

```powershell
.\scripts\gdi\Invoke-GdiDiscovery.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiDeepResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -ContinuationOnly -Mode DryRun
.\scripts\gdi\Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiQualification.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun -Resume
.\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o
```

npm: `npm run test:gdi-canonical-full-e2e-v3-v1` · `npm run test:gdi-exa-who-how-live-adapter-v1`

---

## Post-research report (Part G) — deferred

Items for before/after commercial scorecard + Part G are produced **after** paid GO + Checkpoint A/B execution. This preflight stops before spend.

---

## Status

**`$0` BASELINE LOCKED + LIVE ADAPTER READY + $18 MANIFEST PREPARED**  
**AWAITING_INTEGRATION_CONTROLLER** (PR #613)  
**THEN** founder paid GO `FOUNDER_GO_WYNDHAM_FINAL_COMMERCIAL_CONTINUATION_18USD_20261127`  
**THEN** Checkpoint A only
