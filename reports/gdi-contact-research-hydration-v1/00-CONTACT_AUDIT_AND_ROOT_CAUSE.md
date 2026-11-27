# Wyndham STAGED Contact Audit — Root Cause & Recovery

**Hotel:** Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)  
**Source tip:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_84288a6e`  
**Hydrated tip:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_8927c618`  
**Contract:** `GDI_CONTACT_RESEARCH_AND_HYDRATION` + Stage 3/4 separation

## Exact root causes (not equivalent)

| Classification | What we found | Count / scope |
|---|---|---|
| **VERIFIED_EXISTING_DATA_NOT_MAPPED** | Personal LinkedIn lived in `primaryContact.sourceUrl` / secondary `sourceUrl` while `linkedinUrl` was null. Stage 4 `collectVerifiedChannelsForPerson` ignored `sourceUrl`, so DryRun returned `NO_USABLE_ROUTE` despite LinkedIn. List Contact Details only rendered email/phone → “Not Available” even when LinkedIn existed. | 14 primary + 14 secondary LinkedIn profiles |
| **VERIFIED_EXISTING_DATA_NOT_PERSISTED** | Professional titles for Kathryn Newstead / Brooke Wheeler / Kristine Coan were null or non-title placeholders in tip; settled titles existed in historical packages / WHO evidence. | 3 titles recovered |
| **PUBLIC_CONTACT_RESEARCH_NOT_EXECUTED** | Stage 4 artifact `04-contact-research.json` is DryRun / fixture-oriented; live public HOW for email/phone on `disc_*` corporate rows was not durably executed into the locked-26 tip. Dossiers contain **zero** recoverable person emails for those 14. | 14 email + most phone gaps on `disc_*` |
| **RESEARCH_EXECUTED_NOT_FOUND** | Where WHO/HOW completed with LinkedIn-only identity and Stage 4 public collection found no email/phone. | Overlaps the 14 LinkedIn-only primaries |
| **UI_RENDERING_DEFECT** | List View hardcoded Email/Phone Not Available and omitted LinkedIn SVG; DTO `slimPrimaryContact` dropped `linkedinUrl`. Drawer omitted empty email/phone lines (better) but List looked emptier than reality. | Fixed across List + DTO |
| **TRANSIENT_ENRICHMENT_NOT_PERSISTABLE** | Surfe / Findymail correctly blocked from Full E2E persistence (ESET documents Surfe quarantine). | Preserved — not weakened |
| **CONTACT_IDENTITY_MISMATCH** | None confirmed for tip names vs LinkedIn URLs audited. | 0 |
| **UNVERIFIED_CONTACT_DATA** | Not introduced. No fabricated emails. | 0 |

## Before → After coverage (opportunity primary contacts)

| Metric | Before | After |
|---:|---:|---:|
| Qualified opportunities | 26 | 26 |
| Named contacts | 26 | 26 |
| Verified professional titles | 23 | 26 |
| Verified email | 11 | 11 |
| Verified telephone | 7 | 7 |
| Personal LinkedIn (resolved) | 14 | 14 |
| LinkedIn field `linkedinUrl` set | 0 | 14 |
| Email or telephone | 12 | 12 |
| Usable outreach (email \| phone \| LinkedIn) | 26 | 26 |
| Stage 4 channel annotation | 0 | 26 |

## Recovery performed ($0)

1. Mapped LinkedIn `sourceUrl` → `linkedinUrl` on primary + secondary.  
2. Recovered three settled titles.  
3. Annotated Stage 4 channel research status (email/phone/LinkedIn) without inventing channels.  
4. Fixed WHO/HOW apply + Stage 4 collector + List DTO + List UI LinkedIn SVG.  
5. Rematerialized STAGED tip `…_8927c618` (CURRENT remains unpublished).

## Remaining genuine gaps (targeted research)

14 primary contacts (and their secondaries) still lack verified professional email and/or telephone after exhausting settled dossiers. Proposed budget is in `05-TARGETED_CONTACT_RESEARCH_MANIFEST.json` — **not executed** under this instruction.

## Permanent process

- Stage 3 = who / identity / title / authority.  
- Stage 4 = email / phone / LinkedIn / other person channels; LinkedIn must not terminate other channel research.  
- Stage 4 DryRun must accept LinkedIn from `sourceUrl`.  
- List + Drawer share DTO contact contract including `linkedinUrl`.
