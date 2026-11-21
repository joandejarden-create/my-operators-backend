# GDI Public-Source Email & Phone Discovery — Permanent V1

**Date:** 2026-11-21  
**Branch:** `feat/gdi-public-source-contact-discovery-permanent-v1`  
**Workspace:** exceptional temp WT (registered `ws-gdi` preserved dirty on `feat/gdi-google-style-contact-search-before-surfe-v1`)  
**Paid Surfe / FullEnrich:** NONE  
**Paid public search (Exa/Google):** NONE this evaluation  
**Airtable writes / new fields:** NONE  
**Customer publication / production deploy:** NONE  

---

## 1. Existing capability audit & overlap

### Already on `main` (reuse — do not rebuild)

| Capability | Module |
|---|---|
| Public Contact Discovery 2.0 | `gdi-public-contact-discovery-v1.js` |
| Channel hierarchy + display + dry-run Airtable propose | `gdi-public-contact-channel-hierarchy-v1.js` |
| Search providers (Exa/Serper; paid gated) | `gdi-public-contact-search-providers-v1.js` |
| HTML extraction / PUBLIC_SOURCE origin | `extract-contact-intelligence-from-source.js` |
| Person identity key | `workflow/contact-field-provenance-v1.js` |
| Opportunity persistence fields | `airtable-opportunity-store.js` (`primaryContactEmail/Phone/Name/Role`, payload JSON) |
| 100-contact Exa pilot evidence | `reports/gdi-public-contact-discovery-2-v1/pilot-100/` |

### Active dirty / open work — **do not overwrite or compete**

| Item | Status |
|---|---|
| Branch `feat/gdi-google-style-contact-search-before-surfe-v1` | Dirty local + remote |
| PR [#484](https://github.com/joandejarden-create/my-operators-backend/pull/484) | **OPEN** — Google AI first → Exa → Surfe last resolver (`gdi-missing-channel-resolver-v1`, `gdi-google-grounded-contact-discovery-v1`); gitleaks currently failing |
| Untracked local Apify/Apollo pilots | Present under `ws-gdi` — left untouched |

**Conclusion:** Public-first discovery already exists. This PR adds the **permanent verification standards, bounded workflow wrapper, non-PII metrics, and dry 20-contact eval** that sit on top of the existing stack. Live drawer “Find Email/Phone” Google-first path remains owned by **PR #484** — not duplicated here.

---

## 2. Recommended discovery workflow

1. Require verified named decision-maker (`looksLikePersonName`).  
2. Reuse prior public research for same `personIdentityKey` (name + organization) across hotels.  
3. Recover from Tier-1 authoritative URLs already on the opportunity (free).  
4. Optional paid public search (Exa/Google) only with explicit authorization + effort caps.  
5. Classify channels with founder verification statuses.  
6. Persist only when separately authorized (default OFF).  
7. Offer Surfe on-demand for still-missing channels.  
8. FullEnrich email only after genuine Surfe miss + existing flags (phone FE OFF).

Module: `gdi-public-contact-discovery-workflow-v1.js`

---

## 3. Current approved persistence destinations

- **Canonical:** GDI opportunity / embedded `primaryContact` via existing Airtable opportunity store fields (`primaryContactName/Role/Email/Phone`, `opportunityPayloadJson`).  
- **Origin marker:** `CONTACT_DATA_ORIGIN.PUBLIC_SOURCE` (never Surfe/FullEnrich).  
- **Dry-run upsert propose:** `proposeAirtablePublicContactUpsert` — `execute: false`, no new Contacts table.  
- **No new Airtable tables/fields in this PR.**  

### Schema / permission changes requiring founder approval (STOP)

| Change | Needed? | Notes |
|---|---|---|
| New Contacts table | **Not proposed** | Reuse opportunity-embedded contact |
| New discrete verification-status columns | Optional later | Today: `channelVerificationStatus` inside payload / contact object |
| Production apply of newly found channels | **Requires founder GO** | Eval did not persist |
| Paid Exa/Google budget for broader research | **Requires founder GO** | Eval used $0 |
| Merge/fix PR #484 gitleaks | Required for Google-first CTA path | Separate from this PR |

---

## 4. Contact verification standards

Statuses (permanent):

- `VERIFIED_DIRECT` — named person + org + attributed email/phone + source URL + check date  
- `VERIFIED_ORGANIZATION_ROUTE` — company/role mailbox or labeled org phone  
- `CANDIDATE_UNVERIFIED` — missing source, guessed format, incomplete identity, or provider origin  
- `NOT_FOUND`  
- `STALE_OR_CONFLICTED` — aged verification  

Rules enforced:

- Guessed emails ≠ verified  
- Generic `info@` ≠ personal direct email  
- Provider/Surfe/FullEnrich origins never promoted to verified public  
- Provider values not echoed into verification records  

Module: `gdi-public-contact-verification-v1.js`

---

## 5. Results — 20 real GDI contacts (dry-run)

Mode: **FREE_AUTHORITATIVE_URL_ONLY** · persist=false · Surfe=false · FullEnrich=false · paid public search=false  

Evidence: `EVAL_20_RESULTS.json`

| Metric | Before | After |
|---|---:|---:|
| Verified direct work email | 9 | **10** |
| Verified direct business phone | 9 | **10** |
| Verified org email routes | 2 | 2 |
| Email not found | 9 | 8 |
| Phone not found | 11 | 10 |
| Distinct people | 20 | 20 |
| Multi-hotel people in cohort | 14 | 14 |
| Hotels represented | 6 | 6 |

**New verified finds this run (from existing authoritative URLs only):**  
- Toni Vitale Chan (FIA) — email + phone recovered  

---

## 6. Coverage improvements

- +1 verified direct email / +1 verified direct phone on the 20-contact set without any paid enrichment.  
- Remaining missing channels: **10 email / 10 phone** still Surfe-eligible on demand.  
- Free URL-only pass does **not** invent addresses for the other missing contacts.

---

## 7. Estimated Surfe dependency reduction

| Basis | Value |
|---|---|
| Emails missing verified-direct before | 11 |
| Newly verified direct emails (free URL recovery) | 1 |
| Estimated reduction (this mode) | **~9.1%** of missing emails |
| Broader potential | Prior 100-contact Exa pilot: 53 Surfe avoided / $1.25 spend (separate, paid-search authorized) |

Google-first CTA (PR #484) + authorized paid public search should increase avoidance further; not measured in this $0 run.

---

## 8. Research spending & efficiency

| Item | Value |
|---|---|
| Paid public search | $0 |
| Surfe | $0 / 0 calls |
| FullEnrich | $0 / 0 calls |
| Avg research time / contact | ~832 ms |
| Effort caps | maxQueries=0, maxPages=3 (this eval) |
| Default workflow caps | maxQueries=6, maxPages=4, early stop when direct email+phone |

---

## 9. Cross-hotel deduplication

- `personIdentityKey` = hash(name + organization); requires first+last + org.  
- Same name + different org → different keys.  
- Cohort includes **14** people already linked to multiple hotels.  
- Workflow process cache reuses verified public findings across hotels (unit-tested); this eval runs each person once so runtime reuse count = 0.

---

## 10. Privacy & retention controls

- Metrics ledger stores statuses + hashes only — **no emails/phones**.  
- Surfe/FullEnrich PII never persisted.  
- Provider-origin values excluded from verified public retention.  
- Eval explicitly blocked persistence (`persistAttempted: 0`).  
- Access: admin/research tooling; customer drawer continues to use existing hierarchy + transient Surfe policy.

---

## 11. Schema / permission approvals needed (STOP list)

1. Founder GO before persisting newly recovered public channels from future runs.  
2. Founder GO before any new paid Exa/Google research budget.  
3. Optional: promote `channelVerificationStatus` / `channelLastVerifiedAt` / `channelSourceUrl` to first-class Airtable columns (currently payload/contact fields).  
4. Fix + merge PR #484 for Google-first live CTA.  
5. No production deploy from this PR alone.

---

## 12. PR / CI / tests

- Permanent modules + eval + report on focused branch from `origin/main`.  
- `npm run test:gdi-public-source-contact-discovery-permanent-v1` → PASS (9)  
- Registered `ws-gdi` dirty work **preserved** (not stashed/reset).  

---

## Acceptance mapping

| Criterion | Status |
|---|---|
| Evidence-backed public research before paid enrichment | YES (workflow + hierarchy) |
| Only missing channels → optional Surfe | YES (enrichment decision) |
| No invented contacts | YES |
| Unverified emails not presented as verified | YES |
| No provider PII persisted | YES |
| STOP before schema / paid research / publish / deploy | **YES — waiting founder** |

`AWAITING_INTEGRATION_CONTROLLER`
