# HE50 HI Org Contacts — Bethesda controlled apply (founder report)

**Date:** 2026-11-20  
**Hotel:** Bethesda Marriott `recLuxvwwxID7U2B8`  
**Branch:** `feat/he-contact-intelligence-hi-org-contacts-v1`  
**Standing auth:** `he-5-0-standing-authorization-v1.1-hi-org-contacts`

## Authorization change

Extended HE50 standing conditional authorization to allow pilot-scoped create/update on:

- HI Organizations (employer shells only)
- HI Org Contacts
- HI Contact Channels

**Still blocked:** Surfe persistence, Hotel Census writes, schema changes, Economic/Legal Owner hotel–org edge promotion.

## Bethesda results

| Person | Role | Classification | Property responsibility | Named email | Named phone | LinkedIn | Airtable |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Katherine Button | VP, Asset Management and Investments | RELEVANT_COMMERCIAL_ENTRY_POINT | INFERRED | not recovered | not recovered | verified public | created |
| Jeff Mullen | President and CEO | VERIFIED_ORGANIZATION_CONTACT | INFERRED | not recovered | not recovered | verified public | created |
| Patrick Reece | Director, Asset Management | RELEVANT_COMMERCIAL_ENTRY_POINT | UNKNOWN | not recovered | not recovered | verified public | created |

### Organization routes (once, not per person)

- `info@mullencap.com` — VERIFIED_ORGANIZATION_EMAIL (General / Switchboard)
- `(610) 742-9167` — VERIFIED_ORGANIZATION_PHONE (SEC Form D)
- `https://mullencap.com/` — organization website

### Ownership distinction (Live)

- Economic owner: **UNRESOLVED** (not promoted)
- Mullen org persisted as **Asset Manager** employer shell only
- No Economic Owner / Legal Owner hotel–org edge created

## Airtable

- **Created:** 1 HI Organization, 3 HI Org Contacts, 6 HI Contact Channels (3 LinkedIn + 3 org routes), 1 HI Graph Evidence scope row
- **Updated:** 0 on idempotent re-apply
- **Read-back:** PASS via `listContactsForHotel` + probe
- **Quarantined:** 0

## Public channel research (GDI PCD 2.0 reuse)

- Provider: Exa only (Serper not configured)
- Spend: **~$0.042** (3 × $0.014)
- Named professional emails recovered: **0**
- Named business phones recovered: **0**
- Surfe: not invoked

## Live vs Proposed

| Surface | Behavior |
| --- | --- |
| Live | HI Org Contacts + HI Contact Channels; property responsibility chips remain INFERRED/UNKNOWN; org routes under corporate_contacts |
| Proposed | Continues to show staged Phase 3 overlay for unpersisted ownership claims |

## Live API check (8080)

`GET /api/hotel-explorer/founder-preview/recLuxvwwxID7U2B8?mode=live`

- decision_makers: Jeff Mullen, Patrick Reece, Katherine Button (`source=HI_ORG_CONTACTS`)
- corporate_contacts.source: `HI_CONTACT_CHANNELS`
- ownership economic_owner.known: false

## Remaining seven HE50 hotels

Process is reusable via standing auth + controlled apply. Bethesda is the first controlled application. Remaining hotels require the same RESEARCH → VERIFY → DEDUPLICATE → APPLY → READ BACK → RENDER cycle; no fixed contact count.

## Certification (Bethesda wave)

| Gate | Status |
| --- | --- |
| CONTACT_IDENTITY_VERIFIED | PASS (public LinkedIn + employer) |
| ROLE_AND_RELATIONSHIP_VERIFIED | PARTIAL (org affiliation yes; hotel property responsibility unconfirmed — correctly labeled) |
| PUBLIC_CHANNEL_RESEARCH_COMPLETE | PASS (bounded Exa pass complete; org routes certified) |
| DIRECT_CHANNELS_EVIDENCE_VERIFIED | FAIL / OPEN (no named direct email/phone yet — not claimed) |
| CANONICAL_CONTACT_PERSISTED | PASS |
| LIVE_CONTACT_TAB_VALIDATED | PASS (API + Live source wiring; UI uses same HCI components) |

## Restrictions honored

No Hotel Census contact SoT · No Surfe · No invented emails · No speculative ownership promotion · No destructive writes · No schema changes
