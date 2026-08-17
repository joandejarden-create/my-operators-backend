# Operator Explorer — Brand Relationship Storage Audit

**Date:** 2026-08-09  
**Live presentation table:** `Operator Setup - Brand Relationships` (`tblWU8UDz2pVh3ss4`) — **8 fields**, **73 rows**  
**Map:** `api/lib/operator-brand-relationships-map.js`  
**Also:** Profile.`brands` link → Brand Basics; Profile.`Brand Families Operated`; Fit brand-depth docs

---

## Where relationships live today

| Location | What it stores | Graph quality |
| -------- | -------------- | ------------- |
| Profile.`brands` | M:N link to Brand Basics | Identity list — **no** current/historical, approval, geography |
| Profile.`Brand Families Operated` | Soft multi/text labels | Display; taxonomy soft |
| `Operator Setup - Brand Relationships` | Explorer **presentation rows** (section/title/body) | Narrative UI — **not** normalized approvals |
| Case Studies.`branded_independent` | Polluted brand/independent tags | Not a relationship graph |
| Claims (brand category) | Sparse structured claims | Promising spine |
| Fit brand-depth model docs | Conceptual depth levels | Not fully materialized as Airtable graph |
| Operator Fit Shortlist.`Brand` | Snapshot text at shortlist time | Workflow only |

---

## Required capabilities vs current model

| Need | Supported today? |
| ---- | ---------------- |
| Operator × Brand | Partial (Profile.brands) |
| Brand parent | Via Brand Basics link if present |
| Current vs historical experience | **No** structured |
| Announced relationship | **No** |
| Geography scope | **No** (except prose) |
| Evidence / verification date | **No** on Profile.brands |
| Explicit approval (if known) | **No** reliable |
| Approval scope | **No** |
| Project-specific approval separate | Correctly **not** on master (Class 3) — must stay outreach/deal |

---

## Risk

Treating one operated hotel (or one Profile brand link) as **universal brand approval** is explicitly forbidden by publication policy and Fit governance.

---

## Recommendation

**Yes — eventually require** a normalized table such as:

`Operator Intelligence - Brand Relationships`

Minimum fields (conceptual): Operator, Brand, Brand Parent, Relationship Type (Operates / Operated historically / Announced / Approved if explicit), Geography Scope, Evidence, Verification Date, Current/Historical, Confidence, Limitations, Publication Status.

**Do not** replace Explorer presentation table — keep presentation rows for Brand tab UX.  
**Do not** create the intelligence table in this audit phase.

Near-term: use Claims + Assignments (future) + Profile.brands as hybrid; mark Profile.brands as **display / soft Fit**, not approval.
