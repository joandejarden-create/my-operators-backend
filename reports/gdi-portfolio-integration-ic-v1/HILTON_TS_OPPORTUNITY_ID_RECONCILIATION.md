# Hilton Times Square — Opportunity-ID Reconciliation (IC)

**Hotel:** Hilton New York Times Square (`rec35fExUxCClpOP6`)  
**As of:** 2026-11-24  
**STAGED activation:** NOT performed  
**Sources:** PR #592 paid-research candidate + contact matrix; PR #594 portfolio FQO funnel

## Count conflict

| Source | Count | Notes |
|---|---:|---|
| #592 final certified candidate package | **6** | `gdi_hilton_ts_paid_research_v1_20261124` · pointerActivated=false |
| #594 research FQO PASS funnel | **9** | Automated gate on research inventory — not a package |
| Verified competitive Westin overflow | **1** | `gdi_opp_267d801f2fa39f9e` (SSSP 76th) |
| #594 competitive PASSes | **0** | Funnel mislabeled Westin row as HOTEL |

**Do not replace 6 with 9. Do not discard the competitive Westin record.**

## Package authority

- **Version:** `gdi_hilton_ts_paid_research_v1_20261124`
- **Publication ID:** `gdi_pubpkg_rec35fExUxCClpOP6_c5ed1a17fb3ccca0`
- **Bundled path:** `lib/group-demand-intelligence/bundled-publication-packages/rec35fExUxCClpOP6/gdi_hilton_ts_paid_research_v1_20261124/`
- **#592 certification:** PASS (integrity / named-contact / quality gates of that wave)
- **Hardened FQO re-eval (IC):** requires evidence URL + future-pathway fixes in FQO contract (`sources[].url`, `Date.parse(Date.now())` NaN bug) plus contact-matrix overlay for LinkedIn/email fields stripped from some package `primaryContact` blobs

## ID-level matrix

| Opportunity ID | Title | #592 candidate | #594 funnel | Market scope (authoritative) | Named person | Person-specific channels | IC decision |
|---|---|---|---|---|---|---|---|
| `gdi_opp_8ae344c41d87f94b` | 15th Annual Boutique Hotel Investment Conference | YES | HOLD (channel on raw research) | HOTEL | Ariela Kiradjian | Phone + LinkedIn (matrix; package email is generic inbox — rejected) | **RETAIN in #592 package** — channel via phone/LinkedIn |
| `gdi_opp_f07d86ab80b5f956` | NYSSBA Annual Convention & Education Expo | YES | PASS | HOTEL | Jennifer Kearney | Email + phone | **RETAIN** — BOTH |
| `gdi_opp_3b2b6d838d977415` | Spring Road Conference | YES | PASS | HOTEL | Josh Cacchione | Email + phone | **RETAIN** — BOTH |
| `gdi_opp_84fc450962c52470` | NAMT Fall Conference + Festival of New Musicals | YES | PASS | HOTEL | Adam Grosswirth | Email + phone | **RETAIN** — BOTH |
| `gdi_opp_267d801f2fa39f9e` | 76th Annual Meeting (SSSP) | YES | PASS (as HOTEL) | **COMPETITIVE** | Michele Smith Koontz | Email + phone + LinkedIn (matrix) | **RETAIN as COMPETITIVE** — incumbent Westin New York at Times Square; overflow / stay-to-play / next-cycle only; do not pitch displacement |
| `gdi_opp_5282e7a3223f2741` | SCBWI Winter Conference | YES (paid recovery) | HOLD | HOTEL | Sarah Baker (+ Kim Turrisi secondary) | LinkedIn only (matrix); package primaryContact missing LinkedIn | **RETAIN in package with channel repair** — LinkedIn is person-specific; package body must carry LinkedIn before activation |
| `gdi_opp_790a7cea8c321ada` | (research funnel only) | NO | PASS | HOTEL | — | — | **NOT in certified package** — do not activate from funnel alone |
| `gdi_opp_d82354f276fb05ba` | (research funnel only) | NO | PASS | HOTEL | — | — | **NOT in certified package** |
| `gdi_opp_7b02dca9bafd5f7a` | (research funnel only) | NO | PASS | HOTEL | — | — | **NOT in certified package** |
| `gdi_opp_d81e07432b06190d` | (research funnel only) | NO | PASS | HOTEL | — | — | **NOT in certified package** |
| `gdi_opp_9f8287e4d3fa23b9` | (research funnel only) | NO | PASS | HOTEL | — | — | **NOT in certified package** |

## Set math

- Intersection (#592 ∩ #594 PASS): **4** — `f07d86…`, `3b2b6d…`, `84fc45…`, `267d80…`
- #592 only: **2** — Boutique (`8ae344…`), SCBWI (`5282e7…`)
- #594 only: **5** — funnel extras; **excluded from activation batch** until packaged + FQO-certified

## Competitive Westin (must preserve)

From #592 `04_COMPETITIVE_RECOVERY.json` + contact matrix:

- **ID:** `gdi_opp_267d801f2fa39f9e`
- **Incumbent:** Westin New York at Times Square
- **Pathway:** EVIDENCED_OVERFLOW_OPPORTUNITY / future-cycle
- **Contact:** Michele Smith Koontz — `mkoontz3@utk.edu`, `865-689-1531`, LinkedIn `/in/michele-smith-koontz-28b9029`
- **Sales posture:** overflow / stay-to-play / next cycle — not displacement of Westin host

## Activation posture (IC)

| Item | Value |
|---|---|
| Activatable package for Hilton | `gdi_hilton_ts_paid_research_v1_20261124` **only after** hardened FQO package certification PASS |
| Qualified count to present to founder | **6 package IDs** (5 Hotel + 1 Competitive) — not 9 |
| Funnel-only PASSes | Informational HOLD for packaging |
| STAGED pointer | **NOT activated** |

## STOP

No STAGED/CURRENT pointer writes. No Airtable. No additional paid research.
