# Hotel Explorer — Owner & Decision-Maker Intelligence | Capability Audit V1

**Date:** 2026-11-19  
**PR:** #463  
**Apply:** blocked (staged dry-run only)

## 1. Existing capability (before this loop)

| Capability | Status |
| --- | --- |
| HI Org Contacts / HI Contact Channels schema | Established; Phase 3 dry-run CREATE for 3 MREC people + org email/address |
| Founder Preview proposed contacts VM | Names/titles only; no LinkedIn; peer-equal cards; CEO visually primary by order |
| Surfe reveal API | Present; user-initiated; no persist; capability endpoint |
| Free-source LinkedIn research | Missing from staged package |
| Property-specific ranking | Missing — seniority/order driven |
| Recommended First Contact honesty label | Missing |

## 2. Improved workflow (implemented)

`Existing Airtable → Public research → Verification → Staged canonical persistence → Surfe on explicit click`

Shared engine: `lib/hotel-intelligence/contact-intelligence/property-decision-maker-engine-v1.js`

## 3. Ranking methodology

Documented factors (not opaque scores alone): verified hotel responsibility, AM/ownership authority, hotel/portfolio evidence, LinkedIn, direct channels. Seniority-only CEO capped. PRIMARY requires CONFIRMED property responsibility.

## 4. Bethesda recommendations (staged)

| Priority | Person | Class | Property responsibility |
| --- | --- | --- | --- |
| Suggested first | Katherine Button | Relevant commercial entry point | Inferred (portfolio + AM role) |
| Alternative | Jeff Mullen | Owner decision authority | Inferred (org portfolio; not AM assignment) |
| Alternative | Patrick Reece | Relevant commercial entry point | Unknown |

LinkedIn verified via mullencap.com/about. Personal email/phone unresolved (public). Org fallback: info@mullencap.com.

## 5. Airtable mapping

- LinkedIn → HI Contact Channels (`kind: linkedin`, person subject) — dry-run JSON staged  
- People remain HI Org Contacts (Phase 3 package)  
- Evidence → HI Graph Evidence / Sources tab citations  
- **No production apply**

## 6. Surfe

Enabled as Find Contact Details fallback; passes name, company, title, LinkedIn URL. No auto calls. No credits spent in validation of this package.

## 7. Generalization

Engine is hotel-agnostic. KGPV continues on golden-demo CI path (204 on founder-preview contacts for non-Bethesda). Same ranking helpers apply when other hotels publish staged research packages.
