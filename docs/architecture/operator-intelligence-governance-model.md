# Operator Intelligence — Governance Model

**Status:** Accepted for calibration (local prototype)  
**Date:** 2026-08-03  
**Principle:** Founder approves methodology and material exceptions — not every routine factual record.

---

## Operating loop (target)

```text
Source discovery
→ Source qualification
→ Claim extraction
→ Claim normalization
→ Evidence classification
→ Conflict detection
→ Confidence assignment
→ Publication decision
→ Operator profile update
→ Scoring refresh
→ Periodic source refresh
```

This assignment implements architecture, rules, local calibration data, and Fit v2 overlay — **not** a fully autonomous production pipeline.

---

## Publication classes

### Class 1 — Auto-publish verified objective facts

May publish automatically when evidence threshold is met.

**Examples:** operator identity; official website; parent company; current managed properties; current operating countries; regional offices; hotel segments represented; brands currently operated; announced openings; documented conversions; approximate key count from authoritative source; urban/resort classification; mixed-use / branded-residence presence; named executives; documented property examples.

**Evidence threshold (starting rule):**

- **One authoritative primary source** that directly supports the claim, **or**
- **Two reliable, independent, consistent sources**

Primary source must directly support the claim (not an AI summary or search snippet).

### Class 2 — Auto-publish with evidence label

Publish automatically with a retained qualification label.

**Labels:** Operator Reported · Portfolio Supported · Independently Referenced · Dealality Derived · Historical Evidence · Announced but Not Yet Operating

**Examples:** pre-opening experience; institutional-owner experience; commercial specialization; claimed regional capability; governance approach; owner-service model; development pipeline; general brand relationship; operating philosophy; centralized support capability.

These influence Evidence Confidence differently from Class 1 verified facts.

### Class 3 — Internal only until project-specific validation

Never present as confirmed owner-facing facts from general online research alone:

Property-level financials (RevPAR Index, GOP, EBITDA, flow-through, forecast accuracy); owner satisfaction; current organizational capacity; proposed leadership/regional team; management/centralized fees; contract terms; performance tests; owner approval rights; project-specific brand approval; project-specific operating interest; competitive conflicts; brand-managed availability for a specific project; willingness to operate a specific hotel.

Collect during outreach, references, proposal comparison, or project diligence.

### Class 4 — Never automatically infer

Never auto-infer:

- Global approval from one brand relationship  
- Regional approval from an unrelated market  
- Current approval from a historical relationship  
- Active operating presence from strategic interest  
- Strong performance from portfolio growth  
- Luxury expertise from one luxury hotel  
- Conversion expertise from an unrelated reflagging  
- Local resources from company scale  
- Current capacity from announced growth  
- Brand-management availability from third-party management experience  
- Positive owner satisfaction from a long contract  
- Project-level financial performance from awards or reviews  

These generate **unknowns** or **validation questions**.

---

## Founder review model

### Founder reviews

- Changes to source hierarchy  
- Changes to publication thresholds  
- New evidence classes  
- New controlled-vocabulary values  
- Material scoring methodology changes  
- Conflicting authoritative sources that materially affect ranking  
- Sensitive or potentially damaging operator claims  
- Major profile changes that could materially change Top-5 results  
- Approval of new Airtable tables or fields  
- Approval of production release waves  
- Periodic sample-based quality reviews  

### Founder does **not** approve routinely

- Official property facts  
- Standard geography supported by authoritative sources  
- Standard operating-structure facts  
- Current property examples  
- Official brand relationships  
- Normalized key counts  
- Standard hotel classifications  
- Source refreshes that do not materially change the profile  

---

## Exception queue specification

| Field | Description |
| ----- | ----------- |
| Operator | Name + stable ID |
| Claim | Claim ID / subject |
| Existing value | Current published / local value |
| Proposed value | Candidate value |
| Sources | Source IDs |
| Conflict type | e.g. current vs historical |
| Potential scoring impact | High / Medium / Low / None |
| Publication class | 1–4 |
| Reason for escalation | Why not auto-resolved |
| Recommended resolution | Keep / Replace / Label / Internal / Reject |
| Reviewer | Role |
| Review status | Open / In review / Resolved / Deferred |

High-impact conflicts must enter this queue. Do not resolve silently.

---

## Domain boundary

Operator Intelligence is a **separate domain layer** sharing PI Source Library / governance levels with Brand Explorer. It does **not** reuse Brand Status, Scene7 gallery contracts, or PRIMARY_RELEASE lists.
