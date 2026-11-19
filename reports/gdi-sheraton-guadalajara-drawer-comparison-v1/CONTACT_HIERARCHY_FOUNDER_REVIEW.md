# GDI 3.0 — Contact email & phone hierarchy (Founder review)

**Hotel:** Sheraton Guadalajara Expo (`recCAGqkERSXgiku4`)  
**Audited:** 2026-11-19T21:11:46.378Z  
**Inventory:** 79 opportunities  
**PR gate:** #462 remains OPEN — local-only; **no production Airtable writes**, no Surfe, no paid enrichment, no competing PR.

## Hierarchy applied

1. Verified named direct commercial buyer / decision-maker  
2. Verified named commercially relevant influencer / referral  
3. Verified organizational / department route  
4. Published company switchboard / contact form / business route  
5. On-demand enrichment only for unresolved channels (Surfe — session-only)

Display labels (no card redesign): **Direct Business Email**, **Direct Business Phone**, **Organizational Contact Route**, Find Email / Find Phone / Find Email & Phone.

## Sheraton results (after native public recovery)

| Metric | Count |
| --- | ---: |
| Named contact | 79 |
| Classed as direct buyer | 39 |
| Classed as relevant influencer/referral | 40 |
| Public direct email | 11 |
| Public direct phone | 6 |
| Organizational fallback | 11 |
| LinkedIn / profile URL | 60 |
| Channels recovered this pass | 32 |
| Proposed Airtable upserts (dry-run) | 19 |
| Still Surfe-eligible (missing direct channel) | 71 |

Demo-state distribution (A–D): {"D":68,"A":6,"B":5}

## EUROGUSS MEXICO 2027 / Gissella Palma (regression)

| | Before | After |
| --- | --- | --- |
| Email | — | gissella.palma@nmna.us |
| Phone | — | +1 (770) 618-5833 |
| Org route | — | info@euroguss-mexico.com |
| Surfe CTA | Find Email & Phone | none (complete) |
| Source | — | https://www.euroguss-mexico.com/contacts |
| Role | NAMED_RELEVANT_INFLUENCER | NAMED_RELEVANT_INFLUENCER |
| Recovery | | PUBLIC_CHANNELS_RECOVERED / changed=true |

Public source: [euroguss-mexico.com/contacts](https://www.euroguss-mexico.com/contacts) lists Gissella Palma — Sales Manager — `gissella.palma@nmna.us` — `+1 (770) 618-5833`, plus org `info@euroguss-mexico.com`.

## Side-by-side drawer (original LEFT / cleaned RIGHT)

Preview: `http://localhost:8095/group-demand-intelligence-drawer-comparison.html?opportunityId=paid3_565ce4588e6d`  
Hard-refresh cache `gdi-drawer-cmp-v8-contact-hierarchy`.

| State | Example | LEFT (original package) | RIGHT (cleaned hierarchy) |
| --- | --- | --- | --- |
| **A** | EUROGUSS / Gissella Palma `paid3_565ce4588e6d` | Name only → Find Email & Phone | Direct Business Email + Direct Business Phone + Organizational Contact Route; **no** Surfe CTA |
| **B** | CX Forum / D. Torres `disc_7a1c3c3c8a3d` | Missing or incomplete channels | Email shown → **Find Phone** only |
| **C** | *(none stable this pass after person-scoped fix; phone-only would show Find Email)* | — | — |
| **D** | Latam Business School `disc_2eb37f395de2` | Named person, thin channels | Org route `info@latam.university` + Find Email & Phone when direct still missing |
| **E** | Teatro Diana / Karla Zapata `disc_3f80a42ccf84` | Named contact | Classed `NAMED_RELEVANT_INFLUENCER` — authority not invented |
| **F** | Any Surfe-eligible row | Gated Find Email & Phone | Same CTA; session-only; **not** in Airtable proposals |
| **G** | Airtable reuse | Not live-queried this pass | Proposed upserts ready; execute blocked |

## Validation states (drawer)

| State | Meaning | How to demo |
| --- | --- | --- |
| A | Public email + phone | EUROGUSS / Gissella after recovery — both shown, **no** Find Email & Phone |
| B | Email only | CX Forum `disc_7a1c3c3c8a3d` → Find Phone |
| C | Phone only | Phone recovered, email missing → Find Email |
| D | Named, no direct channel | Latam `disc_2eb37f395de2` + org route + enrichment CTA |
| E | Influencer, not assumed buyer | Role class `NAMED_RELEVANT_INFLUENCER` with honest why-relevant copy |
| F | Surfe session-only | Unchanged gated CTA; never persisted |
| G | Existing Airtable verified | Reuse when present — this pass did **not** live-query Airtable; 19 dry-run upserts prepared |

## Proposed Airtable upserts (DRY-RUN ONLY)

- Count: **19**
- Target: existing GDI opportunity / embedded contact intelligence (no new Contacts table)
- Includes: person, company, role, opportunity association, classification, public email/phone, org route, source URL, verification status, last verified date, provenance=`PUBLIC_SOURCE`, `surfeDerived=false`
- Policy: do not overwrite newer verified data with stale/blank; never guess email formats
- **Execute:** blocked until founder-authorized persistence gate

Machine-readable: `CONTACT_HIERARCHY_MATRIX.json`

## STOP

Awaiting founder review. No production Airtable writes, no Surfe runs, no publication, no competing PR while #462 is open.
