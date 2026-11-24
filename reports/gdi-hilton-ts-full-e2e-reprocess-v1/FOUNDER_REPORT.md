# Hilton New York Times Square — Full E2E Reprocess + Competitive Recovery

**Contract:** `gdi-hilton-ts-full-e2e-reprocess-v1`  
**Hotel:** Hilton New York Times Square (`rec35fExUxCClpOP6`)  
**Spend:** $0 (existing evidence reuse only)  
**Agent:** OFF  
**STAGED pointer:** NOT activated  
**CURRENT / publish / Airtable / Research UI / PR #590:** untouched  

**Status:** STOP — awaiting founder review before any pointer promotion

---

## Executive verdict

| Metric | Count |
|---|---:|
| Original discovered | **45** |
| Original research-qualified (non-DISQUALIFIED) | **44** |
| FINALIZATION PASS (candidate STAGED) | **8** |
| HOLD FOR RESEARCH | **25** |
| REJECT / HISTORICAL | **12** |
| Competitive recovered from existing evidence | **1** |
| Hotel-scope PASS | **7** |
| Competitive-scope PASS | **1** |

The original 44 remain historical research records. Only **8** pass current commercial + named-person finalization standards for a local STAGED **candidate** package. Competitive recovery from the existing store yields **one** evidenced overflow pathway (SSSP → Westin New York at Times Square). Broader Manhattan competitor booking recovery needs separate paid research (budget below).

---

## 1. Original research inventory manifest

- Snapshot: `ORIGINAL_OPPORTUNITIES_SNAPSHOT.json`
- Full 45-row manifest: `01_ORIGINAL_45_MANIFEST.json`
- Source: `data/group-demand-intelligence/hotels/rec35fExUxCClpOP6/opportunities.json` (not overwritten)
- Research version: `market-level-discovery-nyc-v1` (updatedAt 2026-10-06)
- All canonical opportunity IDs preserved (`10_PRESERVED_IDS.json`)

---

## 2. Existing 44 quality assessment

See `02_ORIGINAL_44_QUALITY_ASSESSMENT.json`.

Notable founder examples:

| Opportunity | Disposition | Why |
|---|---|---|
| LA 2028 Olympics | **REJECT** | Los Angeles geography; CLOSED / ROOM_DEMAND_TOO_SMALL; no named contact |
| Tech Conference Venues in New York 2026 | **REJECT** | WEAK / CONTACT_NOT_RELEVANT (Easy RFP venue list); no named person |
| PLUS Employment Practices Liability Symposium | **HOLD** | Demand + NYC dates exist, but primary is org desk (“PLUS Headquarters”), not a verified named person |
| 76th Annual Meeting (SSSP) | **PASS (COMPETITIVE)** | Named Meeting Manager; incumbent **Westin New York at Times Square 2026**; overflow / future-cycle pathway |

---

## 3. PASS / HOLD / REJECT matrix

`03_PASS_HOLD_REJECT_MATRIX.json`

**ORIGINAL 44 → FINALIZATION PASS 8 → HOLD 25 → REJECT 12**  
(plus 1 original DISQUALIFIED inside the discovered 45)

### PASS (8) — local STAGED candidate only

| Scope | Opportunity | Named primary |
|---|---|---|
| HOTEL | 15th Annual Boutique Hotel Investment Conference | Ariela Kiradjian (Partner & COO) |
| HOTEL | SIOR Fall Event | Katie Hollis (Director of Events and Programs) |
| HOTEL | NYSSBA Annual Convention & Education Expo | Jennifer Kearney (Convention and Events) |
| HOTEL | Spring Road Conference | Josh Cacchione (Senior Manager of Partnership and Events) |
| HOTEL | ARIAS U.S. Fall Conference | Larry Schiffer (Executive Director) |
| HOTEL | Scale Healthcare Leadership Conference | David Kolb (Vice President) |
| HOTEL | NAMT Fall Conference + Festival of New Musicals | Adam Grosswirth (Member Services Director) |
| COMPETITIVE | 76th Annual Meeting | Michele Smith Koontz (Administrative Officer & Meeting Manager) |

Senior titles are **not** auto-labeled DIRECT_BUYER; buyer-class hints remain evidence-derived (e.g. EXECUTIVE_SPONSOR / EVENT_MEETINGS / GENERAL_ORGANIZATION_CONTACT).

---

## 4–5. Competitive recovery + incumbent evidence

`04_COMPETITIVE_RECOVERY.json`

| Field | Value |
|---|---|
| Opportunity | 76th Annual Meeting — Society for the Study of Social Problems |
| Label | **EVIDENCED_OVERFLOW_OPPORTUNITY** |
| Incumbent | **Westin New York at Times Square** (2026 annual meeting evidenced in contact rationale) |
| Hilton pathway | Overflow / housing / stay-to-play — **not** immediate displacement of contracted host |
| Contact | Michele Smith Koontz — Meeting Manager |
| HOW | Direct work email `mkoontz3@utk.edu` + office phone (public org contact page) |

No other competitor-hotel bookings were present in the Hilton Times Square research store under $0 reuse.

---

## 6–7. Contact verification + public HOW

`05_CONTACT_VERIFICATION_MATRIX.json`, `06_HOW_COVERAGE.json`

| Signal (full 45) | Count |
|---|---:|
| Plausible named primary | 27 (after anti-junk) |
| Named coverage finalize-eligible | ~27 |
| Personal/direct-style email | 13 |
| Org-email only | 3 |
| Phone | 8 |
| Person LinkedIn | 0 |

PASS cohort: every row has a plausible named primary; secondary persons largely undocumented → recorded as `ATTEMPTED_NO_SECOND_VERIFIED` from existing evidence (honest, no fabrication).

---

## 8. Commercial priority breakdown

`07_COMMERCIAL_PRIORITY_BREAKDOWN.json`

PASS rows are predominantly historical WATCHLIST / research priorities with evidence-backed next actions synthesized only when missing. **Sales Ready remains independent** — package approvalStatus is `CANDIDATE`, not Sales Ready promotion.

---

## 9. Hotel vs Competitive finalization counts

| Scope | PASS |
|---|---:|
| HOTEL | 7 |
| COMPETITIVE | 1 |
| **Total finalized candidate** | **8** |

---

## 10. Versioned local STAGED candidate package

| Field | Value |
|---|---|
| publicationVersion | `gdi_hilton_ts_reprocess_v1_20261124` |
| Path | `lib/group-demand-intelligence/bundled-publication-packages/rec35fExUxCClpOP6/gdi_hilton_ts_reprocess_v1_20261124/package.json` |
| Report copy | `08_STAGED_CANDIDATE_PACKAGE.json` |
| lifecycle | CUSTOMER_RENDERED / approvalStatus **CANDIDATE** |
| STAGED.json pointer | **absent** (not activated) |
| Integrity | PASS |
| Named-contact package assert | PASS |

---

## 11. IDs preserved

`10_PRESERVED_IDS.json` — all 45 IDs retained; PASS/HOLD/REJECT partitions reference original IDs only. No duplicate identities created.

---

## 12. Research gaps + proposed paid budget (founder approval required)

| Gap | Proposed work | Est. USD |
|---|---|---:|
| Manhattan competitor booking ledger thin | Targeted Exa for confirmed blocks at Westin NYTS, Marriott Marquis, Sheraton NY, peers — next-cycle / overflow only | **$8** |
| HOLD cohort named-WHO gaps | Public WHO/ALT for HOLD rows with NYC demand + dates (Agent still OFF unless separately approved) | **$5** |
| Person LinkedIn / secondary contacts | Public HOW completion for PASS + high-value HOLD | **$3** |

**Total proposed (optional):** ~$16 — not executed.

---

## 13. Recommended next steps (activation)

1. Founder review PASS 8 for commercial realism (especially ED/VP roles → outreach posture).  
2. Optionally authorize paid competitive + WHO gap research above.  
3. After approval, Integration Controller may activate STAGED pointer to `gdi_hilton_ts_reprocess_v1_20261124` (separate GO).  
4. Do **not** publish CURRENT until founder Sales Ready / publication gates pass.  
5. Keep Research inventory (45/44) as historical; do not imply Research = finalized.

---

## Certification

`09_CERTIFICATION.json` → **ok: true**

- Original 45/44 preserved  
- No `opportunities.json` mutation  
- No paid research / Agent OFF / no Airtable  
- No Hilton STAGED pointer  
- Peer STAGED pointers (Condesa / LaLuna / Memmo / Bethesda) unchanged  
- PR #590 / Research view not modified  

---

## Governance reminder

Authorized work completed: $0 reprocess, local candidate package, regression certification.  
**STOP before pointer promotion.**
