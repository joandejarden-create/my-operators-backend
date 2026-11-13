# GDI Opportunity Card Quality — Founder Report

**Date:** 2026-11-13  
**Contract focus:** Buyer contacts, display titles, Rooms/Catering icons, toolbar alignment  
**Hotels verified:** Bethesda Marriott · AC Hotel A Coruña · Wyndham San Diego Bayside  

## Root cause

Airtable is the live opportunity primary. Bethesda FS mirror already held recovered named contacts, `GUESTROOMS_ONLY` / theses, and cleaner titles, but Airtable `opportunityPayloadJson` lagged (`UNKNOWN` / org-desk placeholders, missing revenue-stream class). List projection then double-sanitized rows and demoted evidence-backed icons to muted/`REVENUE_STREAM_UNCONFIRMED`.

Prior presentation fixes were not lost — they never reached Airtable-primary reads consistently.

## Fixes shipped

1. **FS-mirror enrichment** (`fs-mirror-opportunity-enrichment-v1.js`) on Airtable reads — recover named contacts, revenue-stream class/theses, concise titles without inventing demand.
2. **Airtable sync** for Bethesda — 52 rows upserted from FS enrichment (dry-run + apply).
3. **Central display-title policy** — `normalizeGdiCustomerDisplayTitle` (research-commentary strip + professional Title Case, acronym/minor-word safe).
4. **Contact gap copy** — `Contact Research Required` (no implication that qualification is complete).
5. **Revenue-stream icon idempotency** — second sanitize/presentation pass no longer wipes active icons.
6. **Toolbar tokens** — shared `--gdi-toolbar-control-height: 3rem` for Property / Weekly / Type / Market Scope; CDP measured **48×48px, Δh=0, Δtop=0**.

## Bethesda sales-team inventory (67)

| Metric | Before | After |
|--------|--------|-------|
| Displayed opportunities audited | 67 | 67 |
| Missing buyer / “Buyer Not Yet Identified” | 12 | **0** |
| Existing contacts recovered (FS → live) | — | **12** (focus set) / **52** Airtable enrichments |
| Still requiring new contact research | — | **0** on Bethesda 67 |
| Titles corrected (display policy) | — | focus examples + inventory via normalizer |
| Demand-icon both-muted | 40 | **0** |
| Rooms only / Catering only / Both | — | **49 / 7 / 11** |
| Lacking rooms+catering evidence (both muted) | 40 | **0** on Bethesda 67 |

### Focus examples (recovered)

| Opportunity | Contact | Icons | Display title |
|-------------|---------|-------|---------------|
| NIH SBPO Vendor Outreach Sessions | Annette Owens-Scarboro | Rooms | NIH SBPO Vendor Outreach Sessions |
| CTN / Clinical Trials Network Lodging 2027 | Quandra Blackeney | Rooms | CTN / Clinical Trials Network Lodging 2027 |
| Georgetown Homecoming Weekend 2027 | Vanessa Otarola Kehoe | Rooms | Georgetown Homecoming Weekend 2027 |
| Woman's Club of Bethesda | Diane Robertson | Rooms | Woman's Club of Bethesda |
| University of Maryland Alumni & Family Weekend | Natalie Ross | Rooms | University of Maryland Alumni & Family Weekend |

## Multi-hotel (no property-specific patches)

| Hotel | Count | Contact Research Required | Both icons muted | Active icons |
|-------|-------|---------------------------|------------------|--------------|
| Bethesda | 67 | 0 | 0 | 67 |
| AC Hotel A Coruña (`rec2PVBDavppGpenm`) | 0 sales-team | n/a | n/a | empty inventory |
| Wyndham San Diego Bayside | 47 | 18 (honest gaps) | 6 | 41 |

AC Coruña’s selectable catalog hotel currently has no sales-team customer-ready rows; presentation code paths are hotel-agnostic. Wyndham retains legitimate “Contact Research Required” where no named person exists.

## Toolbar alignment

Desktop CDP: select height 48, Market Scope height 48, tops identical. Reset View uses the same control height token under the label stack.

## Regression protection

- Fourteen demand segments, Market Scope All/Hotel/Competitive, Commercial Priorities, CSV, sales feedback untouched in intent.
- No fabricated contacts; no paid research; no auto-publication.
- PR #223 authorization safeguards preserved.
- Tests: `npm run test:gdi-opportunity-card-quality-v1`, `npm run test:gdi-bethesda-88-contact-quality-completion-v1`.

## Screenshots

`reports/gdi-opportunity-card-quality-v1/screenshots/`

- `01-desktop-toolbar.png` / `gdi-card-quality-01-desktop-toolbar.png`
- `gdi-card-quality-02-cards.png` — rooms / both / named contacts
- `gdi-card-quality-06-contact-research-*.png` — Contact Research Required
- `gdi-card-quality-07-mobile-toolbar.png`
- `gdi-card-quality-wyndham-icons-contacts.png` (after capture)

## Sync artifact

`reports/gdi-opportunity-card-quality-v1/fs-mirror-airtable-sync-recLuxvwwxID7U2B8-applied.json` — 52 applied, 0 failed.
