# FOUNDER CLOSEOUT — GDI Canonical Full E2E V3

**At:** 2026-11-27  
**Stream:** GDI (`C:\Dev\dealality\ws-gdi`)  
**Branch:** `feat/gdi-canonical-full-e2e-v3-who-how`  
**Governance:** NO_PAID_RESEARCH · NO_AIRTABLE · NO_STAGED · NO_CURRENT · NO_FINDYMAIL · NO_SURFE · Bethesda + CLEAN11 freezes ON

---

## 1. Existing five-stage mappings vs new functionality

| Stage | Existing (reused) | New in V3 |
|---|---|---|
| 1 DEMAND_DISCOVERY | 14-segment matrix, leisure LDI, CAD, PLP, screening dispositions, CP soft guidance | `Invoke-GdiDiscovery.ps1` + checkpoint |
| 2 OPPORTUNITY_DEEP_RESEARCH | COMMERCIAL_FULL_E2E (PR #612), Exa job store, budget ledger | `Invoke-GdiDeepResearch.ps1`; missing buyer → Stage 3 |
| 3 WHO/HOW_RESOLUTION | buyer-WHO restore, named coverage, entry-point class | Dedicated WHO/HOW Agent contract; mandatory 2 entry-point fallback; status taxonomy |
| 4 PROFESSIONAL_CONTACT_RESEARCH | public HOW workflow, verification | Stage-4 policy; channel provenance; Surfe never auto-persist |
| 5 COMMERCIAL_QUALIFICATION | six-rule FQO + drawer | Audited entry-point reachability rule; PRIMARY + ALT 1/2 drawer |

**No parallel research engine.**

---

## 2. Canonical scripts and tested PowerShell commands

All under `scripts/gdi/`:

| Script | Tested command |
|---|---|
| `Invoke-GdiDiscovery.ps1` | `-HotelId recnPB3Q02NO8Ma1o -Mode DryRun` |
| `Invoke-GdiDeepResearch.ps1` | `-HotelId recnPB3Q02NO8Ma1o -Mode DryRun` |
| `Invoke-GdiWhoHow.ps1` | `-HotelId … -ContinuationOnly -Mode DryRun` |
| `Invoke-GdiContactResearch.ps1` | `-HotelId … -Mode DryRun` |
| `Invoke-GdiQualification.ps1` | `-HotelId … -Mode DryRun` |
| `Invoke-GdiFullE2E.ps1` | `-HotelId … -Mode DryRun` **PASS** |
| `Invoke-GdiValidateFullE2E.ps1` | `-HotelId …` **PASS** |

Node CLI: `scripts/gdi/run-gdi-full-e2e-v3-stage-cli-v1.mjs`  
npm: `npm run test:gdi-canonical-full-e2e-v3-v1` / `npm run gdi:v3:validate`

Live paid mode is intentionally refused in this CLI until the authorized Exa adapter tranche (exit 3) — prevents accidental spend.

---

## 3. WHO/HOW Agent contract

`lib/group-demand-intelligence/workflow/gdi-who-how-agent-contract-v1.js`

- Family: `WHO_HOW_RESOLUTION_V3`
- Input: Stage-2 dossier only (`buildWhoHowAgentInputFromDossier`)
- Statuses: `VERIFIED_DIRECT_BUYER` | `VERIFIED_BUYER_INFLUENCER` | `VERIFIED_ENTRY_POINTS_2` | `VERIFIED_ENTRY_POINT_1` | `ORGANIZATIONAL_ROUTE_ONLY` | `CONTACT_RESEARCH_REQUIRED` | `NO_CREDIBLE_CONTACT_IDENTIFIED`
- Separates identity verification from buying-authority verification
- Never invents people/emails; seniority ≠ buyer

---

## 4. Alternative-entry-point research specification

When no verified direct buyer:

1. Find up to **two** distinct, independently verified commercially relevant people  
2. Rank `ENTRY_POINT_1` / `ENTRY_POINT_2`  
3. Each requires name, title, org, relationship, why credible route, evidence URL, sales approach, and explicit **authority NOT verified**  
4. If only one found → second = `NOT_FOUND`  
5. If none → `RESEARCH_CONTINUATION` / `NO_CREDIBLE_CONTACT_IDENTIFIED`  
6. Unrelated executives rejected (`commerciallyRelevant: false`)

---

## 5. Contact verification and enrichment policy

`gdi-contact-research-stage-v1.js`

- After WHO/HOW only  
- Public-first channels with provenance  
- One usable route sufficient (email **or** phone **or** LinkedIn)  
- Reject invented emails, company LinkedIn as person, switchboard-as-direct  
- Findymail: separate auth only  
- Surfe: never auto from Full E2E; persist refused  

---

## 6. FQO treatment of non-buyer entry points

`gdi-fqo-entry-point-reachability-v1.js` — rule id `FQO_NAMED_REACHABILITY_ALLOWS_VERIFIED_ENTRY_POINT_V1`

May satisfy **only** `NAMED_COMMERCIAL_PERSON` + `PERSON_SPECIFIC_CHANNEL` when:

- Verified identity + commercially relevant  
- Why-relevant + credible pursuit action  
- Person-specific outreach route  
- Buyer label remains entry-point (not mislabeled DIRECT_BUYER)

Demand / hotel fit / future pathway / hotel action gates remain independent. Unrelated contacts fail.

---

## 7. GDI drawer screenshots / browser verification

Fixture page: `reports/gdi-canonical-full-e2e-v3/drawer-browser-fixtures.html`

Cases rendered:

- A — verified direct buyer (Dede Kassel)  
- B — two entry points after “Hotel Decision-Maker Not Yet Verified”  
- D — unresolved / NOT_FOUND  

UI hook: `whoHowDrawerPresentationHtml` in `public/js/group-demand-intelligence/dealality-gdi-ui.js` (preserves existing design language; V3 wins when `whoHowDrawer` present).

Browser verified at `http://127.0.0.1:8766/` (local fixture server). Evidence: `reports/gdi-canonical-full-e2e-v3/browser-evidence/who-to-contact-cases-A-B.png` — Case A direct buyer + Case B two entry points with authority disclaimer. Case D unresolved present on same page.

---

## 8. Durable job and resume tests

- Checkpoint file: `reports/gdi-canonical-full-e2e-v3/<hotel>/checkpoints/*-e2e-v3-checkpoint.json`  
- Test **H**: settled WHO/HOW job id reused with `billed: false`  
- Test **G**: `reused: true` fixture path  
- Stage outcomes: PASS / HOLD / PARTIAL / FAILED / SKIPPED / NOT_STARTED  

---

## 9. Weekly Change integration

- Scenario **L** models buyer change detection (`weeklyChange.buyerChanged`)  
- Orchestrator preserves prior verified contacts unless contrary evidence  
- Does not force full rediscovery for one missing contact  
- Shares demand-generator identity across hotels without mixing hotel-specific FQO (scenario **K**)

Full weekly-change scheduler wiring remains on existing GDI weekly infrastructure; V3 checkpoints record `lastVerifiedAt` for stale-contact governance.

---

## 10. Cost and provider authorization controls

- DryRun default everywhere  
- Live requires `-AuthorizePaidResearch` **and** Mode=Live  
- This V3 CLI still refuses Live spend (fail-closed) until paid Exa WHO/HOW adapter tranche  
- Stage budget metrics slots ready (discovery / deep / who-how / contact / enrichment)  
- Budget exhaustion → PARTIAL (existing COMMERCIAL_FULL_E2E semantics)  

---

## 11. Full regression results

```
npm run test:gdi-canonical-full-e2e-v3-v1
→ 18/18 PASS (A–O + orchestrator + stage order + drawer hooks)

.\scripts\gdi\Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun → EXIT 0
.\scripts\gdi\Invoke-GdiWhoHow.ps1 … -ContinuationOnly -Mode DryRun → EXIT 0
.\scripts\gdi\Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o → EXIT 0
```

---

## 12. PR / Integration Controller handoff

| Field | Value |
|---|---|
| PR | *(filled after `gh pr create`)* |
| Expected merge | Integration Controller remote merge only |
| Local branch | `feat/gdi-canonical-full-e2e-v3-who-how` |
| Base | `main` |
| Post-merge | `dealality:post-merge-sync --stream gdi` |

GDI chat does **not** merge.

---

## 13. Recommended initial Wyndham WHO/HOW paid tranche (after merge)

**Proposed auth:** `FOUNDER_GO_WYNDHAM_WHO_HOW_V3_TRANCHE_<DATE>`

| Item | Guidance |
|---|---|
| Scope | Top 25 WHO/HOW continuation from prior $0 plan (associations/gov/sports) |
| Mode | Stage 3 only — **no** rediscovery of 161 |
| Ceiling | **$5.00** hard (≈25–40 Agent WHO/HOW calls @ $0.10) |
| Then | Stage 4 public contact DryRun; Findymail only under separate auth |
| Stop | Ceiling / settled-job reuse / no FQO-gate relaxation |
| Success | ≥10 of 25 recover buyer or ≥1 entry point; drawer shows correct labels |

Combine with prior Final Commercial Continuation ($18) only if founder prefers a single umbrella auth — otherwise keep WHO/HOW tranche separate and small for first live proof.

---

## Status

**IMPLEMENTATION READY — AWAITING_INTEGRATION_CONTROLLER** (after PR open)

Dirty worktree preserved (unrelated paths not committed).
