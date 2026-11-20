# HE Public Contact Hierarchy + Surfe session-only (V20 / HE-QL-032)

**Date:** 2026-11-19  
**Stream:** Hotel Explorer (`ws-hotel-explorer`)  
**PR:** #463  
**Aligned with:** GDI founder directive — Contact email & phone hierarchy (same labels / Surfe policy)

## Bethesda public research (Katherine Button · Jeff Mullen · Patrick Reece)

| Person | Identity | LinkedIn | Direct email | Direct phone | Org route | Property responsibility | Surfe CTA |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Katherine Button | VERIFIED (mullencap.com/about) | [linkedin.com/in/katherine-button](https://www.linkedin.com/in/katherine-button/) | Gap | Gap | info@mullencap.com · (610) 742-9167 · contact page | INFERRED (not Bethesda-assigned) | Find Email & Phone |
| Jeff Mullen | VERIFIED | [linkedin.com/in/jeffmmullen](https://www.linkedin.com/in/jeffmmullen/) | Gap | Gap | same org routes | INFERRED (sponsor CEO; not AM assignment) | Find Email & Phone |
| Patrick Reece | VERIFIED | [linkedin.com/in/patrick-reece-375b8937](https://www.linkedin.com/in/patrick-reece-375b8937/) | Gap | Gap | same org routes | UNKNOWN | Find Email & Phone |

### Rejected (not public-verified)

- RocketReach / AeroLeads / Prospeo masked or guessed emails & phones  
- Invented formats (`jmullen@`, `jeff.mullen@`)  
- Wrong-company channels (`legacyparking.com` for Patrick)

### New public org evidence this pass

- SEC Form D business phone `(610) 742-9167` — **Organizational Contact Route only** ([filing](https://www.sec.gov/Archives/edgar/data/2080980/0002080980-25-000001.txt))  
- Mullen Hospitality team page lists Katherine / Patrick as affiliate advisors ([team](https://mullenhospitality.com/about/team/))

## Display / Surfe

- Labels: **Direct Business Email** · **Direct Business Phone** · **Organizational Contact Route**  
- Surfe: Find Email | Find Phone | Find Email & Phone by missing direct channel only  
- Surfe results: session-only, `persist: false`, cleared on remount/reload, not indexed in Sources  
- Mock provider remains available for no-cost state F demos  

## Persistence

- Dry-run upserts: `reports/hotel-explorer/phase4-quality/HE-BETH-PUBLIC-CONTACT-UPSERTS-DRY-RUN-V1.json`  
- LinkedIn channels dry-run (prior): `HE-BETH-CONTACT-LINKEDIN-CHANNELS-DRY-RUN-V1.json`  
- **Execute blocked** — Phase 3 Airtable apply unauthorized  
- `surfeDerived: false` on all proposals  

## Regression states

| State | Meaning | HE coverage |
| --- | --- | --- |
| A | Public email + phone | Unit fixture (Gissella pattern) |
| B | Email only → Find Phone | Unit |
| C | Phone only → Find Email | Unit |
| D | Named + org fallback | Bethesda Katherine / Jeff |
| E | Influencer, no invented authority | Patrick Reece |
| F | Surfe session-only | Reveal API + client `_sessionSurfeByPerson` |
| G | Reuse / dry-run upsert | Upsert JSON + LinkedIn dry-run |

## Gates

- `npm run test:he-public-contact-channel-hierarchy-v1`  
- `npm run test:he-contact-intelligence-directory-ux-v1`  
- `npm run test:he-product-quality-gates-v1` (includes HE-QL-032)

## STOP

No production Airtable apply. No live Surfe credits required for QA (mock). No deployment.
