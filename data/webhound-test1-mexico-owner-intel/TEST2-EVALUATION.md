# Webhound Test 2 Evaluation — Outreach-Ready Packages (Oleum + Venado)

**Session:** `ea7ffe5d-b5f2-4e46-bcfe-56835c22551d`  
**URL:** https://webhound.ai/session/ea7ffe5d-b5f2-4e46-bcfe-56835c22551d  
**Status:** completed (`budget_complete`)  
**Spend:** **$4.84** of $5.00 authorized (funded credits)  
**Rows:** 2 · Expected fields: 61 · Fill rate: ~82%  
**Alerts:** none  
**Pastizales:** correctly skipped (budget used on Oleum + Venado)  
**Governance:** Candidate intelligence only. Not ingested into Airtable / SoT.

## Decisive question

> Can Webhound turn a known early project signal into an actionable Dealality owner relationship?

**Answer: Partially yes.** Especially for **institutional** ownership (Venado / Tortuga). Opaque private SPVs (Oleum) get much further than Test 1 but still lack a verified direct outreach channel.

## Executive verdict

**Partial pass — Promising enrichment; not fully outreach-ready across both targets**

Compared with Test 1 (0 ownership chains, 0 contacts):

| Capability | Test 1 | Test 2 |
|------------|--------|--------|
| Primary SEMARNAT Gaceta PDFs accessed | No (press about filings) | **Yes** (both projects) |
| Applicant conclusive | Partial | **Yes** |
| Ownership chain | Failed | **Venado High; Oleum Medium** |
| Named decision-maker | 0 | **2** |
| LinkedIn / professional route | 0 | **1 strong (Venado); Oleum none for founder** |
| Verified business email / phone | 0 | **0** |
| Company-level contact path | 0 | **Both (address / website / LinkedIn org)** |

## Criteria scorecard

| # | Criterion | Oleum | Venado |
|---|-----------|-------|--------|
| 1 | Project researched | Yes | Yes |
| 2 | Primary government filing accessed | **Yes** (DGIRA/0022/26, 0004/24) | **Yes** (DGIRA/0028/26) |
| 3 | Filing applicant identified | Oleum Joint Venture, S. de R.L. de C.V. | Residencial Punta Venado, S.A. de C.V. |
| 4 | Legal owner / SPV resolved | Yes (medium; Sol Yucatan citing folio 156020) | Yes as promovente/SPV (land title less certain) |
| 5 | Parent / controlling owner | Founders Serrano Lluch + Anguiano Campos (medium); no corp parent | **High:** KSL Capital + Rodina → Tortuga → Residencial Punta Venado |
| 6 | Property-owner vs developer | Largely same entity | Distinguished (platform vs SPV) |
| 7 | Relevant decision-maker | Luis Francisco Serrano Lluch | **Leo Schlesinger (CEO)** |
| 8 | LinkedIn / professional route | No (founder); staff LI only | **Yes** |
| 9 | Verified business email | **No** | **No** |
| 10 | Verified business phone | **No** | **No** |
| 11 | Company-level contact path | Registered address Tijuana + Cancún ops signal | Website + company LinkedIn + UK registry address |
| 12 | Still pre-decision (reasonable) | Conditional Pre-Decision Candidate | Conditional Pre-Decision Candidate |
| 13 | Owner has other hospitality ops | Possible Ritz-Carlton license link (low/med) | **Strong** (~15–23 resorts / $2B platform) |

## Package assessment

### Venado — near outreach-ready (stronger package)

- Chain: Residencial Punta Venado → Tortuga Resorts UK → Tortuga (KSL + Rodina)
- Decision-maker: Leo Schlesinger, CEO, LinkedIn verified in-session
- Usable path: LinkedIn + Tortuga corporate surface (no direct email/phone)
- Dealality angle: institutional platform relationship may matter more than the single project
- Caveat: `important_evidence_gaps` text was stale (still says “no decision-maker”) — ignore that field’s lag; contact fields are populated

### Oleum — ownership/decision-maker advanced; not cold-outreach ready

- Primary filings solid; applicant = property owner per secondary investigative source
- Founders named; Serrano Lluch as legal representative (Dateas + Sol Yucatan)
- Contact: registered address only; no founder LinkedIn/email/phone
- Confidence medium; US foreign-investment vehicle and Desarrolladora Oleum link unresolved
- Corridor: adjacent to Venado but **different ownership** (correctly not assumed from adjacency)

## Cost / value (actual $4.84)

| Metric | Value |
|--------|--------|
| Cost / project researched | **$2.42** |
| Cost / ownership-resolved project | **$2.42** (2/2) |
| Cost / project with relevant decision-maker | **$2.42** (2/2) |
| Cost / project with usable contact path | **~$4.84** (Venado LinkedIn-grade; Oleum address-only) |
| Cost / fully outreach-ready package (email or phone + ownership) | **N/A (0)** |
| Cost / near outreach-ready package (ownership + DM + LinkedIn/company route) | **~$4.84** (Venado) |

## Independent status call

| Project | Webhound status | Independent lean |
|---------|-----------------|------------------|
| Oleum | Conditional Pre-Decision Candidate | Agree — recent MIA auth, no public brand; private planning possible |
| Venado | Conditional Pre-Decision Candidate | Agree — same; institutional owner may already run parallel brand talks |

Both remain **candidate intelligence**, not “Strong Pre-Decision” without brand/operator disconfirmation beyond absence of announcement.

## Key learnings

1. **Enrichment >> rediscovery** for this use case at $5.
2. **Primary Gaceta access** fixed Test 1’s press-only weakness.
3. **Institutional owners** (PE/JV platforms) resolve to parent + named exec + LinkedIn much faster than opaque private SPVs.
4. **Verified email/phone** remains the hard failure mode; LinkedIn + company route is the realistic $5 ceiling for many Mexico targets.
5. **Corridor adjacency ≠ shared ownership** — Webhound correctly separated Oleum vs Tortuga.

## Is another test justified?

**Only with explicit approval.** Possible optional threads (not launched):

- Contact-hardening for Serrano Lluch / Anguiano Campos (Oleum)
- Pastizales third package (only if still wanted)
- Tortuga relationship map beyond Venado (portfolio outreach strategy)

**Remaining uncommitted balance: $50.16 — do not spend without approval.**

## Artifacts

- `test2-session-meta.json`
- `test2-rows.jsonl`
- `test2-rows-compact.json`
- `test2-prompt.txt` / `test2-schema.json`
- `TESTING-LEDGER.md`
