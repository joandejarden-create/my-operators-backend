# Dealality Decision Radar (GTM internal)

**Status:** Stage 1 — schema / field map / validation contract  
**Base:** `AIRTABLE_GTM_BASE_ID` (same GTM base as Owner Targets)  
**SoT modules:** `lib/gtm-owner-target/decision-opportunity-field-map.js`, `decision-opportunity-validate.js`, `decision-opportunity-schema-spec.js`  
**Ensure:** `node scripts/ensure-gtm-decision-opportunities-schema.mjs` (dry-run default; `--apply` explicit)

---

## Purpose

Dealality Decision Radar finds:

> **live owner decisions, not generic hotel owners.**

It is an **internal acquisition research** system. It must not become a mass hotel-owner CRM, a cold-email engine, or a customer-facing marketplace.

---

## North Star

One live owner opportunity  
→ 5–10 counterparties approached  
→ 3+ real responses  
→ 2+ proposals side by side  
→ owner confirms the process was materially better.

Optimize for **qualified live decisions**, not record count.

---

## Relationship to Target OS

| System | Job |
|--------|-----|
| **Decision Radar** | Finds the owner decision (pre-Dealality). |
| **Target OS** | Runs the brand/operator process **after** the owner opportunity enters Dealality. |

They **must remain separate**.

Do **not** modify:

- `api/target-list.js`
- customer Deals / Brand Deal Requests / Outreach Hub product workflow
- Hotel Census production writes
- Brand Explorer / Operator Explorer

---

## Core pipeline

```text
Signal
→ Hotel / Project
→ Owner
→ Decision
→ Decision Makers
→ Warm Path
→ Evidence
→ Score
→ Founder Action
```

Stage 1 implements the **data foundation** only (tables, maps, validation). It does **not** seed opportunities, run discovery, score automatically, or send outreach.

---

## Identity rule

A **Decision Opportunity** represents:

> **One hotel/project + one meaningful strategic decision window.**

It does **not** represent:

- an owner in general
- an entire portfolio
- a news article
- a contact
- every future decision at the same hotel

The same property may have **multiple** Decision Opportunities across time (e.g. 2026 acquisition/reflag vs 2031 management renewal).

Greenfield projects may have **no** Lead Property link and still be valid opportunities.

---

## Fact vs inference standard (mandatory)

### `NOT_PUBLICLY_IDENTIFIED ≠ NOT_SELECTED`

**Brand Status / Operator Status = Not Publicly Identified** means only that public sources do not name a brand/operator.

It does **not** mean the decision remains open.

Correct pattern:

- `Brand Status` = Not Publicly Identified  
- `Decision Still Open` = Uncertain (or Yes only with caution)  
- `Decision Open Confidence` = Probable / Inferred / Unknown — **never Confirmed from absence alone**  
- `Why Now` explains that further qualification is required  

**Absence of evidence is not evidence that a deal remains open.**

Evidence may also **Support Closed / Too Late** (e.g. exclusive signing announcement).

---

## Airtable tables

### `Decision Opportunities`

Linked to existing GTM:

- **Owner Target** → Owner Targets  
- **Lead Property** → Properties (optional)  
- **Decision Makers** → Contacts (reuse; do not duplicate)  
- **Duplicate Of** → self  
- **Decision Opportunity Evidence** → inverse of Evidence link  

### `Decision Opportunity Evidence`

Child provenance rows. Required fields include Source URL/Name, Supports Field, Evidence Confidence, Evidence Direction (`Supports Open` | `Supports Closed / Too Late` | `Neutral / Context`).

**Evidence Confidence** uses High / Medium / Low (aligned with Partner Source Library Source Quality).

**Decision Open Confidence** uses Confirmed / Probable / Inferred / Unknown (field-level fact vs inference — separate from Opportunity Score).

---

## Trigger mapping (Radar ↔ GTM Deal Trigger)

Owner Targets use snake_case `Deal Trigger` (`VAL_GTM_DEAL_TRIGGER`). Radar uses Title Case `Trigger`:

| GTM Deal Trigger | Decision Radar Trigger |
|------------------|------------------------|
| `conversion` | Conversion Candidate |
| `reflag` | Reflag / Operator Mismatch |
| `operator_rfp` | Operator Selection Proxy |
| `new_build` | New Development |
| `development_pipeline` | Development Pipeline |
| `independent_unbranded` | Independent / Unbranded |
| `brand_renewal_window` | Brand Renewal Window |
| `portfolio_standardization` | Portfolio Standardization |
| `sale_process` | Acquisition / Sale Process |
| `recent_open_branded` | Recent Open Branded (Late) |
| `none_known` | Unknown |

Extra Radar triggers (filings/news) have no CoStar Deal Trigger equivalent (e.g. Planning / Environmental Approval).

Branding-decision timing (`pre_decision` / `post_decision` / `uncertain`) maps to Decision Stage via `MAP_BRANDING_TIMING_TO_DECISION_STAGE` in the field map (for future Lane A seeds — not applied in Stage 1).

---

## Lifecycle status

`Discovered` → `Researching` → `Qualified` / `Monitor` / `Founder Review` → … → `Outreach Ready` → …

**Stage 1 does not auto-promote to Outreach Ready.**

Validation contracts:

- **Qualified / Founder Review:** project identity, country, trigger, likely decision type (or Unknown + rationale), Why Now, Decision Still Open, Decision Open Confidence, ≥1 evidence row. Founder Review also requires Decision Stage.
- **Outreach Ready (contract only):** Owner Target, decision-maker or actionable warm path, Decision Still Open ≠ No, evidence present, Founder Reviewed.

Warm paths are **manual / evidence-backed only** — never inferred automatically.

---

## Score foundation

Fields exist for Opportunity Score, Score Band, Score Explanation, Scored At.

**Do not populate speculative scores in Stage 1.** Full Decision Radar scoring is deferred.

---

## Schema ensure

```bash
node scripts/ensure-gtm-decision-opportunities-schema.mjs
node scripts/ensure-gtm-decision-opportunities-schema.mjs --dry-run
# After founder approval of dry-run report:
node scripts/ensure-gtm-decision-opportunities-schema.mjs --apply
```

Idempotent. Reports conflicts if an existing field has an incompatible type (no silent mutation). Report: `reports/ensure-gtm-decision-opportunities-schema.json`.

---

## Validation

```bash
npm run test:gtm-decision-opportunity
# or
node scripts/test-gtm-decision-opportunity-validate.mjs
```

Use `validateDecisionOpportunityWrite` / `validateDecisionOpportunityEvidenceWrite` before any future writes.

---

## Explicitly deferred (not Stage 1)

- Lane A opportunity generation from branding-decision targets  
- Lane B Webhound discovery / auto-ingest  
- News crawling → opportunity creation  
- Dedupe engine  
- Full opportunity score engine  
- Contact enrichment automation  
- Warm path inference  
- Weekly founder report  
- Customer-facing UI  
- Target OS / product integration  
- Automated email/outreach  

---

## Data contract snapshot

| Item | Value |
|------|--------|
| Base | `AIRTABLE_GTM_BASE_ID` |
| Tables | `Decision Opportunities`, `Decision Opportunity Evidence` |
| Field maps | `MAP_DECISION_OPPORTUNITY`, `MAP_DECISION_OPPORTUNITY_EVIDENCE` |
| Validator | `lib/gtm-owner-target/decision-opportunity-validate.js` |
| Visibility | Always `internal_only` |
| Expected founder output (later) | 5–10 opportunities/week worth review — not thousands of owners |

---

## Change impact

**High** (Airtable schema + acquisition workflow contract). Rollback: do not `--apply`; if applied, stop using tables / archive views; do not delete without backup.
