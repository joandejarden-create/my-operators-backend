# Operator Fit — Brand-Managed vs Third-Party Pathway Diagnostic

**Date:** 2026-08-09  
**Status:** Architecture documentation only  
**Do not modify Operator Fit code or scoring**

---

## Problem

Owners often choose among:

1. **Brand + Brand-Managed Operator** (e.g. Marriott + Marriott Managed / MxM)  
2. **Brand + Third-Party Operator** (e.g. Marriott + Highgate)  
3. **Brand + Third-Party Operator** (e.g. Marriott + Hotel Equities)

These are **different operating pathways**, not interchangeable “Marriott-experienced” candidates.

---

## Required future comparison object

```text
Pathway = Brand × Operating Structure × Operator Candidate Type × Operator Entity
```

Examples:

| Pathway | Brand | Structure | Candidate Type | Operator |
| ------- | ----- | --------- | -------------- | -------- |
| A | Marriott | Brand-managed | Brand-Managed Operator | Marriott International (Managed) |
| B | Marriott | Franchise + third-party operator | Third-Party Operator | Highgate |
| C | Marriott | Franchise + third-party operator | Third-Party Operator | Hotel Equities |

---

## What must differ in evaluation (future)

| Dimension | Brand-Managed path | Third-Party path |
| --------- | ------------------ | ---------------- |
| Structure fit | Scores against brand-managed / brand agreement structure | Scores against third-party / franchise+operator |
| Brand relationship | “Brand Managed Capability” on own brands — **not** third-party approval | Documented operate-under-brand experience / depth |
| Project approval | Still **not** inferred; confirm brand will manage **this** asset | Brand approval of **this** operator for asset — still outreach |
| Geography | Managed footprint / offices for brand entity | Operator Market Presence |
| Assignments | Brand-managed hotels as comps | Third-party operated hotels as comps |
| Economics / fees | Brand management economics (Class 3 until outreach) | Operator fee proposal (Class 3) |

---

## Explicit non-equivalence rules

- Do **not** give MxM “brand compatibility points” as if it were a third-party approved for Marriott.  
- Do **not** treat Highgate’s Marriott experience as equivalent to MxM availability.  
- Do **not** auto-include Brand-Managed candidates when owner excludes brand-managed structures.  
- Unconfirmed BM availability → Eligible With Conditions + validation item (existing founder decision 2.3).

---

## Explorer vs Fit ownership

- Explorer publishes BM facts + scoped brand capability.  
- Fit derives pathway scores per deal.  
- Never store deal pathway scores on Operator Master.
