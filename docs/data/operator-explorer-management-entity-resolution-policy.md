# Operator Explorer — Management Entity Resolution Policy

**Date:** 2026-08-10  
**Status:** Policy draft — no Airtable writes  
**Related:** `reports/operator-explorer-brand-managed-universe-normalized.md`

---

## Rule

> Create **one Operator Master** for each genuinely distinct operating company/platform that an owner could evaluate as a **management counterparty**.

Do **not** create duplicate Masters merely for:

- marketing names  
- divisions  
- acronyms  
- management programs  
- individual brands  

---

## Display vs internal entity

| Layer | Pattern |
| ----- | ------- |
| **Explorer display name** | Commercial parent + clear managed lens where needed, e.g. `Marriott International (Managed)` |
| **Management entity / program alias** | Stored as alias / relationship context, e.g. `Managed by Marriott (MxM)`, `Hilton Management Services` |
| **Brand scope** | Individual brands (Conrad, Courtyard, …) live on **Brand Relationships** / Assignments — not as Operator Masters |

---

## Standard resolutions

| Seen as | Canonical Master | Notes |
| ------- | ---------------- | ----- |
| Marriott / Marriott International / MxM / Managed by Marriott | Marriott International (Managed) | One Master |
| Hilton / Hilton Worldwide / Hilton Management Services | Hilton (Managed) | One Master |
| Accor / AccorHotels / Accor Group | Accor (Managed) | One Master |
| IHG / InterContinental Hotels Group | IHG Hotels & Resorts (Managed) | One Master |
| Minor Hotels / Minor International / Minor Hotel Group | Minor Hotels (Managed) | One Master |
| NH Hotels / NH Collection (as operator counterparty) | **Do not** create NH Operator Master | Brand scope under Minor + Brand Managed Capability rows |
| Hyatt / Hyatt Hotels Corporation | Hyatt (Managed) when created | One Master; HVO is separate non-pathway parent |
| Iberostar / Iberostar Hotels & Resorts | Grupo Iberostar | Integrated owner/brand/operator — **not** a second `(Managed)` Master |
| Soft brands (Preferred, SLH, LHW) | **No** Brand-Managed Operator Master | Not management counterparties |

---

## When a separate Master **is** justified

Create a new Master only if **all** are true:

1. Owner would contract with that legal/commercial platform as a distinct counterparty; **and**  
2. It is not merely a brand, program name, or division alias of an existing Master; **and**  
3. Operating Model / Management Availability classification applies to that counterparty as a whole.

Examples that usually qualify: Sonesta International, Four Seasons Hotels and Resorts, Highgate (third-party), Arbor Lodging.

Examples that usually do **not**: MxM, HMS, “Marriott Luxury”, “Hilton Luxury Brands”.

---

## Exceptions (document explicitly before creating)

| Case | Decision |
| ---- | -------- |
| Brand-managed vs third-party arm of same parent with separate contracting entities | Separate Masters only with legal/commercial evidence of distinct counterparties |
| Regional JV management company with distinct brand | Separate Master if owners contract the JV, not the parent |
| Owner-operator brand group already Master’d (Iberostar) | Do not also create `(Managed)` twin |

---

## Registry

Extend `lib/partner-intelligence/brand-managed-operator-link-registry.js` aliases for parent → Master resolution.  
Do not proliferate Masters to satisfy Brand Explorer parent-string variants.
