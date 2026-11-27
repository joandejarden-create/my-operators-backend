# Hotel Explorer 5.0 — Release Reconciliation Matrix V1

**Audit time:** 2026-11-27  
**origin/main:** `6bb2f0c8a5fad97c90275974ec87cd013fc8965c` (merge #627)  
**Verify checkout:** `C:\Dev\dealality\_verify_pr627_main` @ `6bb2f0c8a`  
**Live :8080:** serves asset hashes matching main/#627 contact + explorer JS/CSS (NOT Contact CX V2; unified theme CSS 404).  
**FULL_HE_E2E_VALIDATED:** `NO`

## Serving process

| Item | Evidence |
|---|---|
| Listener PID | `17320` (`node server.js`) |
| Served `hotel-explorer.js` sha16 | `bebd7dd36ec9a8a0` → matches `_verify_pr627_main` / hydration WT |
| Served `hotel-contact-intelligence.js` sha16 | `a5ea65bfc02df9d7` → **OLD** “Recommended First” hierarchy |
| Served `hotel-explorer.css` sha16 | `c44a6f8590ccfbbd` → matches main (theme PR #625 differs: `5ef22d0f696beda4`) |
| `he-unified-theme-v1.css` | **404** on :8080 |
| Contact CX V2 (`e73f22e9…`) | Present only on `wt-he-contact-cx-v2` / PR #619 — **not served** |

## Open HE PRs (not in main)

| PR | Branch | Worktree | Status |
|---|---|---|---|
| #619 | `feat/he-contact-cx-v2` | `wt-he-contact-cx-v2` @ `e7518605b` | OPEN — Contact CX V2 |
| #621 | `feat/he-guest-sentiment-disabled-by-founder-v1` | (files in guest-sentiment WT / PR) | OPEN — 11-tab flag |
| #622 | `feat/he50-completion-progress-v1` | `wt-he-guest-sentiment-disabled-v1` | OPEN — docs/scorecard |
| #623 | `feat/he-intelligence-factory-v1-1` | `wt-he-factory-v1-1` @ `104e1ff60` | OPEN — Year Built / Factory |
| #625 | `feat/he-unified-theme-v1` | `wt-he-unified-theme-v1` @ `e1bf707d5` | OPEN — unified theme |
| #616 | `feat/he50-physical-product-canonical-v1` | `wt-he50-physical-product-pr-v1` @ `527dde19c` | OPEN — physical product schema |
| #480 | `feat/he-contact-intelligence-hi-org-contacts-v1` | — | OPEN — older contact path |

## Feature matrix

| Feature | Research / artifact | Source | In main? | On :8080? | Airtable SoT? | Hydrated? | Rendered? | Remaining defect |
|---|---|---|---|---|---|---|---|---|
| Cross-tab shared VM + consistency report | `he-shared-hotel-view-model-v1`, `HE-CROSS-TAB-CONSISTENCY-*` | PR **#627** MERGED | **YES** | Partial (libs present; Live UI still gaps) | HPC rooms 407 YES | Shared VM YES in code | Org portfolio still missing rooms | Org focus row rooms null; economic class still VALIDATED from graph API |
| 11 tabs / Guest Sentiment off | `HE_GUEST_SENTIMENT_ENABLED=false`, flag module | PR **#621** OPEN | **NO** | **NO** (sentiment still referenced) | N/A (flag) | N/A | Likely 12-tab chrome still available | Must merge flag; preserve GS code |
| Contact Intelligence V2 purpose directory | `contact-purpose-sections-v1`, CX V2 JS/CSS | PR **#619** OPEN | **NO** | **NO** — old hierarchy served | 5 Bethesda contacts persisted (prior #617) | Old API path | Old “Recommended First” UI | Replace Live UI with purpose sections; no record-ID title |
| Unified HE theme | `hotel-explorer-theme-tokens-v1.css` + CSS edits | PR **#625** OPEN | **NO** | **NO** (404 / old CSS hash) | N/A | N/A | Inconsistent panels | Merge theme; certify all tabs |
| Physical product (407 / 171 / 236 / 8 / 18719 / 27 / ballroom) | Schema + Bethesda adapter reports | PR **#616** OPEN; meeting-space display already on BP API | Partial | Partial | Rooms **407** on HPC YES; meeting total on BP via `hotel_intelligence_meeting_space`; bed mix / suites **needs #616 + read-back** | Meeting summary on BP YES | Rooms tab needs full inventory DTO | Complete #616 persistence/read-back; no hardcode |
| Year Built 1979 / Reno 2015 (+2017 milestone) | Factory assessment MD; Opening Date / Renovation Date | PR **#623** OPEN | **NO** (API `yearBuilt`/`lastRenovated` empty) | Empty on BP | Opening Date / Reno Date per factory notes — verify live fields | Not mapped on Radar DTO | Overview “Not yet verified” | Merge factory mapping; do not invent 2017 as sole Last Renovated |
| Ownership Proposed vs Validated | Shared VM forces Proposed; graph status VALIDATED | #627 + product UI | Partial | API still `economic_owner_or_group.status=VALIDATED`; PropCo API `UNKNOWN` despite Marbeth name elsewhere | HI graph edge Validated; founder class Proposed | Shared VM remaps; Live ownership payload does not | Risk of Validated badge | Force founder Proposed on all ownership surfaces; surface Marbeth PropCo |
| Org & Portfolio 407 rooms | `owner-portfolio-operator-view-v1` + shared VM | #627 | YES (code) | Portfolio hotel row **rooms undefined** | HPC 407 | **FAIL** focus hydration | Known rooms 0 / unresolved 1 | Finish focus-row shared VM hydration end-to-end |
| Factory E2E gates (persistence/theme/contacts) | `Invoke-HotelExplorerFactory.ps1` | #627 + #623 | Partial (#627 consistency only) | N/A | N/A | N/A | N/A | Extend gates per §9 |

## Live Bethesda API snapshot (:8080)

| Field | Live value | Notes |
|---|---|---|
| Rooms | 407 | HPC / brand-presence |
| Meeting space display | 18,719 sq ft · 27 rooms · Grand Ballroom 4,592 | Provenance `hotel_intelligence_meeting_space` |
| Year built | *(empty)* | Factory mapping not on main |
| Last renovated | *(empty)* | Same |
| Economic owner | Mullen — **VALIDATED** | Must remain **Proposed** in product |
| PropCo | UNKNOWN / null on control payload | Marbeth not surfaced |
| Portfolio focus rooms | undefined | Hydration defect |

## Permanent homes (preserve — do not reset)

| Path | HEAD | Dirty | Note |
|---|---|---|---|
| `ws-hotel-explorer` | `cd11df4cd` detached | **4553** | PRESERVE |
| `my-operators-backend` (IC) | `c448e4529` detached | **82** | PRESERVE |
| `wt-he50-contact-sentiment-restore-v1` | `513360106` | **552** | PRESERVE |
| `_verify_pr627_main` | `6bb2f0c8a` | reports-only | Safe integration base |

## Integration order (controlled)

1. #621 Guest Sentiment flag → 11 tabs  
2. #619 Contact CX V2  
3. Ownership/Org hydration fixes (Proposed + 407 focus)  
4. #616 Physical product persistence/read-back  
5. #623 Year Built / Renovation mapping  
6. #625 Unified theme  
7. Strengthen factory gates + browser certify from clean serve checkout  

`FULL_HE_E2E_VALIDATED` stays **NO** until §10 browser acceptance passes.
