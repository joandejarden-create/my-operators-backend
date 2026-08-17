# Operator Explorer Phase 0–1 — Schema Dry-Run Design

**Date:** 2026-08-10  
**Mode:** Design only — **no Airtable creates/writes**  
**Axes:** Operating Model ⊥ Management Availability (`reports/operator-explorer-brand-managed-universe-normalized.md`)  
**Entities:** 27 deep-calibration companies

---

## 1. Assignments (proposed table dry-run)

**Name:** `Operator Intelligence - Assignments`  
**Why Case Studies insufficient:** polluted selects; missing keys/dates/brand links/currentness/evidence.

| Field (conceptual) | Type | Essential? |
| ------------------ | ---- | ---------- |
| Assignment Key | text | Yes |
| Operator | link → Master | Yes |
| Hotel Canonical Name | text | Yes |
| Hotel Census link | link (optional) | High value |
| Country / City / Region | text or selects | Country essential |
| Brand | link → Brand Basics | High value |
| Brand Parent | text/lookup | High value |
| Keys | number | High value |
| Segment / Chain Scale | select | High value |
| Hotel type / Urban-Resort / Service style flags | select/multi | Optional |
| Development flags (New Build, Conversion, Reflag, …) | multi | High value |
| Owner / Developer | text | Optional |
| Start / End date | date | High value |
| Current / Historical | select | Essential |
| Management Structure | select | High value |
| Evidence Source / Strength | text/select + PI link | Essential |
| Last Verified | date | Essential |
| Link to Case Study | link optional | Optional |

**Dry-run payload shape:** one sanitized assignment row preview per evidenced hotel; no apply.

---

## 2. Brand Relationships (typed) dry-run

**Name:** `Operator Intelligence - Brand Relationships` *(intel — distinct from Explorer presentation table)*

| Field | Notes |
| ----- | ----- |
| Operator | Master |
| Brand | Brand Basics |
| Brand Parent | text/lookup |
| Relationship Type | incl. **Brand Managed Capability**, Operates Under Brand, Historical, Announced, Explicit Approval (scoped) |
| Geography / Region / Segment scope | required for Brand Managed Capability |
| Current / Historical | |
| Offered to third-party owners? | Yes / Selective / No / Unknown |
| Evidence + Last Verified | |
| Publication Status | |

**Never** encode project approval here.

Presentation table `Operator Setup - Brand Relationships` remains Explorer UI rows only.

---

## 3. Market Presence dry-run

**Existing table** — no schema create required for core types.

Dry-run rows for Track 1+2: Country + Market Presence Type + Current/Historical + evidence + Claim ID.

Optional future additive (not created now): City/metro, property count.

---

## 4. Claims dry-run

**Existing table** — prefer select normalization later (not this apply).

Dry-run claim objects: identity / geography / structure / experience / brand / comparable with publication class + conflict status.

Link to PI Source Library: design additive field; URLs acceptable interim.

---

## 5. Sources

Prefer PI Source Library records; local reference captures for Core 5 BM; official development/managed pages for Track 2.

AI/snippets never evidence.

---

## 6. Publication policy / conflicts / readiness

Reuse `lib/operator-intelligence/publication-policy.js` + conflict detector.

Gates remain separate: Research Complete Enough · Explorer Publishable · Fit Data Ready · High Confidence.

Candidate-type / Operating Model / Management Availability are **facts for Explorer**, not Fit scores.

---

## 7. Profile payload (dry-run)

Per entity JSON preview:

```text
identity + Operating Model + Management Availability
+ marketPresence[]
+ assignments[] (deep track only)
+ brandRelationships[] (incl. Brand Managed Capability where applicable)
+ structures / segments (summaries)
+ evidence footnote
+ readiness { research, explorerPublishable, fitDataReady }
```

Brand-managed presentation: same sections; Overview labels Operating Model; Brand Relationships shows scoped capability.

---

## 8. Schema fit checklist (dry-run)

| Need | Fit |
| ---- | --- |
| Operating Model vs Availability | Two fields/concepts — not one Candidate Type dump |
| Scoped BM capability | Typed Brand Relationships |
| Assignments inventory | New Assignments table |
| Geo eligibility | Market Presence |
| Claim audit trail | Claims + PI |
| No duplicate MxM Master | Entity resolution policy |
| Fit pathways later | Diagnostic only — no engine change |

---

## Explicit non-goals this phase

- Airtable field/table creation  
- Operator Fit code/scoring  
- Deep Assignment research apply  
- Owner pilot enablement  
