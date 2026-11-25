# FOUNDER — Commercial Priorities → Canonical Full E2E (V1)

**Contract:** `gdi-canonical-commercial-priorities-run-context-v1`  
**Paid live:** false · **Airtable writes:** none · **STAGED/CURRENT:** unchanged  
**Wyndham San Diego Bayside:** `recnPB3Q02NO8Ma1o`  
**Hilton NY Times Square (leisure matrix prior):** `rec35fExUxCClpOP6` — **SEPARATE_HOTELS_NOT_ALIAS**  
**Bethesda:** frozen · **Wyndham CLEAN 11:** protected

---

## 1. Field-by-field integration truth table

Classification distinguishes **verified runtime** (fixture canonical entry + $0 tests) from code existence.

| Field / signal | Portal save/load | Canonical Full E2E run-context | Discovery plan soft signal | Agent deep research | Hotel fit / sourcing window | Commercial ranking (soft) | Cards / drawers | Weekly Change | Class |
|---|---|---|---|---|---|---|---|---|---|
| Revenue priorities | YES (Tab2 store) | YES — snapshot | PARTIAL — via bridged `commercialPriorities` into segment plan | PARTIAL — ranking/emphasis only; no segment drop | Soft fit band via peak rooms | YES — explained | PARTIAL — preference match lines on processed rows | Version-aware classify helper | **PARTIALLY_INTEGRATED** → now **FULLY_INTEGRATED** for run-context + soft rank |
| Active need periods | YES (need-period store) | YES — open only | PARTIAL — isolation-aware load | Soft timing overlap | Soft timing match | Soft need-date match label | Preference explanations | Change impact labels | **PARTIALLY_INTEGRATED** (campaign executors remain separate) |
| Peak room-block preset | YES | YES → min/max | Soft coreTargetPeakRooms | Soft | Soft fit | Soft | Explained when matched | Versioned | **FULLY_INTEGRATED** (soft) |
| Booking window | YES | YES | Soft notes | Soft | Soft | Soft | Soft | Versioned | **FULLY_INTEGRATED** (soft) |
| Meeting-space need | YES | YES | Soft | Soft | Soft | Soft | Soft | Versioned | **FULLY_INTEGRATED** (soft) |
| Stay duration / weekdays | YES | YES | Soft weekend/weekday focus | Soft | Soft | Soft | Soft | Versioned | **FULLY_INTEGRATED** (soft) |
| Target / priority segments | YES | YES | Soft ordering/emphasis; **does not remove** 14-core | Soft candidate emphasis | Soft | Soft | Soft | Versioned | **FULLY_INTEGRATED** (soft; no silent removal) |
| Excluded segments | YES | Snapshot only | **Must not hard-drop** | No auto-reject | Neutral miss | Soft | Neutral | Versioned | **PARTIALLY_INTEGRATED** (recorded; never reject) |
| Local demand-generator categories | YES | YES (`demandAnchorFocus`) | Soft brief notes | Soft | Soft | Soft | Soft | Versioned | **PARTIALLY_INTEGRATED** |
| Confirmed known accounts | YES | YES | Soft | Soft | Soft | Soft | Soft | Versioned | **PARTIALLY_INTEGRATED** |
| Hotel considerations | YES | YES | Soft notes | Soft | Soft | Soft | Soft | Versioned | **PARTIALLY_INTEGRATED** |
| Business restrictions | YES | YES | Soft conflict flag | Soft flag only | Hard-ops still separate | CONFLICTS label; **no reject** | Soft | Versioned | **PARTIALLY_INTEGRATED** (flag ≠ reject) |
| Profile version / provenance | YES | YES — `profileId` + `currentVersion` | Traceable | Traceable | Traceable | Traceable | Traceable | **CHANGED_HOTEL_PRIORITY** | **FULLY_INTEGRATED** |
| Historical / Replaced need periods | YES | Loaded but excluded from active | Neutral | Neutral | Neutral | Neutral | Neutral | Context only | **FULLY_INTEGRATED** |
| Paid need-date campaign strategy executors | SEPARATE | Not invoked by this PR | — | — | — | — | — | — | **SEPARATE_CAMPAIGN_ONLY** (reuse later; not duplicated) |

**Pre-PR truth (audit):** Tab2 portal + need-date campaign path integrated; canonical Full E2E used config-only commercial hints — **PORTAL_ONLY / SEPARATE_CAMPAIGN_ONLY** for hotel-confirmed Tab2.  
**Post-PR verified runtime:** canonical entry always builds versioned run-context, bridges soft signals into profile resolution, attaches soft alignment on processed rows. Still does **not** invent demand or remove segments.

---

## 2. Existing PowerShell / need-date components already usable

| Component | Path | Reuse |
|---|---|---|
| Leisure NeedPeriodContextJson | `scripts/gdi-leisure/Invoke-GdiLeisure*.ps1` + `Invoke-GdiLeisureModule.ps1` | Pass need-period JSON into leisure modules |
| Strategy selection | `need-period-strategy-selection-v1.js` | Campaign / date-specific strategy pick — **do not duplicate** |
| Dual-track planning | `need-date-research-track-planning-v1.js` | Date-specific vs account-relationship tracks |
| Preference explanations | `hotel-preference-match-explanation-v1.js` | Ranking / drawer copy |
| Timing overlap | `need-period-timing-match-v1.js` | Confirmed overlap only elevates need-date match |
| Lifecycle open/inactive | `need-period-lifecycle-v1.js` | Active vs Replaced/Filled filters |
| Hotel store | `need-period-hotel-store-v1.js` | Local versioned need periods |
| Tab2 contract + store | `hotel-commercial-priorities-contract-v1.js`, `hotel-commercial-profile-store-v1.js` | Canonical profile shape |
| Research bridge | `hotel-saved-commercial-profile-research-bridge-v1.js` | Map Tab2 → soft `commercialPriorities` (+ peak rooms band) |
| Portal API | `api/gdi-need-dates-portal.js` | CRUD for profiles + need periods |

---

## 3. Exact missing canonical connections (before this PR)

1. Canonical Full E2E did **not** load Tab2 saved profiles at run start.  
2. No versioned commercial-priorities **run-context snapshot** artifact.  
3. Peak-rooms preset not mapped into `coreTargetPeakRoomsMin/Max` for fit engines.  
4. Soft preference explanations not attached to canonical `chain.processed`.  
5. Weekly Change lacked a version-aware “priority change ≠ new discovery” classifier.  
6. Need-period campaign strategy executors remain intentionally **out of** canonical paid path (no duplicate executor).

---

## 4. Minimum integration changes (this PR)

1. Enhance research bridge: `peakRoomsPresetToCoreTargetBand` + soft map fields.  
2. New `gdi-canonical-commercial-priorities-run-context-v1.js` — snapshot, soft score, weekly-change classifier.  
3. Canonical entry: build run-context → write `commercial-priorities-run-context.json` → pass profile into chain → soft-align processed rows.  
4. Profile resolution opt-in: `applySavedCommercialProfile=true` (canonical only).  
5. Original chain forwards bridge flags.  
6. $0 test harness — 8 founder scenarios + canonical entry smoke.

---

## 5. Commercial-priority run-context contract

`buildCanonicalCommercialPrioritiesRunContext(hotelId)` returns:

- `snapshot`: revenue priorities, active need periods, peak rooms band, booking window, meeting space, stay/weekdays, target/excluded segments, demand-generator categories, known accounts, considerations, restrictions, provenance, `profileId`/`currentVersion`, flags (`softSignalOnly`, `doesNotLimitDiscovery`, `missingFieldsAreNeutral`, `zeroActiveNeedPeriodsMeansNeutralTiming`)
- Missing fields = **neutral**, never restrictive  
- Suggestions ≠ hotel-confirmed (`savedProfileApplied` requires confirmed profile)  
- Active / commercially open need periods only

---

## 6. Need-date and preference-ranking behavior

| Rule | Behavior |
|---|---|
| Demand qualification (FQO) | Independent — strict six-rule FQO unchanged |
| Commercial alignment | Soft — SUPPORTS / NEUTRAL / CONFLICTS_WITH_RESTRICTION |
| Preference miss | Opportunity **preserved**; never REJECTED |
| Need-date priority boost | Only when timing overlap is confirmed/supported |
| Recurring relationship, no date | May be valuable; **not** labeled verified need-date match |
| Zero active need periods | Neutral timing preference — **not** “no demand” |
| Priority change alone | `CHANGED_HOTEL_PRIORITY` / `CHANGED_COMMERCIAL_FIT` — **not** new discovery |
| Excluded segments | Soft signal only — **no silent segment removal** |

---

## 7. $0 test results

`npm run test:gdi-canonical-commercial-priorities-run-context-v1` → **11/11 PASS** (`TEST_RESULTS.json`).

Scenarios: no prefs; group rooms + need months; catering/social; restrictions; selected segments; weekly priority update; replaced need periods; multi-hotel same opp; canonical entry snapshot write.

---

## 8. Required PRs / merge status

See PR created from this branch. GDI chat does **not** merge — Integration Controller owns merge + post-merge sync.

---

## 9. Remaining live-validation requirements (STOP — not started)

- Live hotel with Tab2-confirmed profile on `recnPB3Q02NO8Ma1o` (Wyndham) — verify run-context loads correct hotel only.  
- Weekly Change UI labels for priority-only re-eval (no paid rediscovery).  
- Card/drawer copy QA for preference match lines on certified inventory.  
- Optional later: wire need-period strategy selection into research **ordering** without invoking paid campaign executors.  
- **Do not** start paid discovery, Findymail/Surfe, STAGED/CURRENT, or Airtable mutations from this task.

**SUCCESS CRITERION:** Every certified canonical Full E2E is guided by current Commercial Priorities + Need Dates while discovering broadly and preserving all credible demand.
