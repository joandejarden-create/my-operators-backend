# Recover COMPLETE Historical Hotel Commercial Priorities Interface

**Date:** 2026-11-12  
**Branch:** `feature/gdi-complete-commercial-priorities-tab-v1`  
**Hotel validation:** Bethesda Marriott (`recLuxvwwxID7U2B8`)

---

## 1. Historical sources investigated

| Source | Result |
|--------|--------|
| All local/remote git branches (need-date, commercial, priorities) | Inspected |
| `feat/gdi-universal-need-date-intelligence-v1` @ `7fa87ebfd` / tip `fb7086b8b` | Need-dates portal only (calendar); **no** commercial form |
| `release/gdi-need-date-v4-customer-presentation` (PR #154) | Presentation-only Need-Date icons — not a management portal |
| Git object search for `hotel-commercial-priorities-app.js` | **Zero blobs** — file never committed |
| Authorized backups (`primary-cutover`, `primary-remaining-source`, `primary-working-state`) | Contract + profile store only; **no** public HCP app |
| Stashes / reflog | No HCP UI payload |
| Agent transcripts / prior PR #213 deliverable | Confirmed HCP app “referenced but never committed” |
| Hotel config JSON `commercialPriorities` | Legacy config schema (`targetSegments`, `weekendFocus`, peak rooms) — **preserved** as suggestion source |
| Contract `hotel-commercial-priorities-contract-v1.js` (commits `64d06cca8`, `385a2d072`) | **Most complete historical definition** of fields/options |

---

## 2. Most complete historical implementation identified

**Not a recoverable frontend app.** The later comprehensive commercial-priorities UI was never stored in git under any filename.

**Authoritative historical artifact = selection-first contract V1:**

- File: `lib/group-demand-intelligence/need-date-demand/hotel-commercial-priorities-contract-v1.js`
- Persistence: `hotel-commercial-profile-store-v1.js` + Airtable `GDI Hotel Commercial Profiles`
- API already accepted most fields; PR #213 Tab 2 UI only exposed a subset

**Recovered calendar UI (unchanged lineage):** `hotel-need-dates-app.js` @ `7fa87ebfd` → Tab 2 need-period calendar (PR #213).

---

## 3. Original branch / commit / filenames

| Kind | Identity |
|------|----------|
| Need-dates portal | `feat/gdi-universal-need-date-intelligence-v1` · `7fa87ebfd` · `public/js/.../hotel-need-dates-app.js` |
| Commercial contract | `64d06cca8` / `385a2d072` · `hotel-commercial-priorities-contract-v1.js` |
| Missing UI | `hotel-commercial-priorities-app.js` — **never in git** |
| Stub HTML title references | Documented in PR #213 founder deliverable; no body committed |

---

## 4. Features missing from PR #213 Tab 2

PR #213 form only had: revenue chips, day preference, specific days, priority segments, restriction notes.

**Missing vs contract (now restored in UI):**

- Preferred peak rooms
- Booking window
- Meeting / catering need
- Stay duration
- Segment mode (all / specific / none)
- Excluded / lower-priority segments
- Local demand audiences (`DEMAND_GENERATOR_CATEGORIES` — historical stand-in for participant sources)
- Business to avoid
- Hotel considerations
- Known accounts list
- Config-derived suggestions (weekend focus, target segments → 14-segment map)
- Draft preservation across calendar re-renders / tab keep-alive
- Supervisor soft boosts for sports/weekend, government, weddings, extended stay, demand audiences

---

## 5. Features recovered / reconstructed

| Feature | Classification |
|---------|----------------|
| Need-period calendar + CRUD | **Recovered** (PR #213 from `7fa87ebfd`) |
| Contract enums + Airtable profile schema | **Recovered** (already on main) |
| Full commercial form binding every contract field above | **Reconstructed from contract** (no historical app to restore verbatim) |
| Weekday/weekend + specific days | Contract-defined; UI completed |
| 14 GDI demand segments + exclusions | Contract-defined; UI completed |
| Demand audiences / participant sources | Contract `DEMAND_GENERATOR_CATEGORIES` |
| Soft supervisor ranking differences | Extended in `selectStrategiesForCampaign` |

**Honesty statement:** This is **not** a byte-for-byte restore of a lost `hotel-commercial-priorities-app.js`. That file was never found. The UI is a faithful reconstruction driven by the committed contract + existing persistence APIs.

---

## 6. Could not recover

- Pixel-perfect historical layout / copy of the uncommitted HCP portal
- Separate weekday-vs-weekend *revenue matrices* (not in contract — only dayPreference + period overrides)
- A dedicated `participantTypes` enum distinct from demand generators / segments (not in contract)

---

## 7. Before / after feature comparison

| Capability | Early need-dates (`7fa87ebfd`) | PR #213 Tab 2 | This delivery |
|---|---|---|---|
| Need periods / calendar | Yes | Yes | Yes |
| Weekday / weekend | No | Partial (day select) | Full + specific days + soft ranking |
| Peak rooms / booking / stay / meetings | No | No | Yes |
| 14 demand segments | No | Priority only | Priority + exclude + mode |
| Demand audiences | No | No | Yes |
| Avoid / considerations / known accounts | No | Notes only | Yes |
| Airtable profile persistence | Store existed | Partial fields saved | Full field patch |
| Supervisor soft ranking | Segments/revenue partial | Partial | Weekend sports vs weekday meetings verified |

---

## 8. Existing hotel data preserved

- Bethesda hotel-config `commercialPriorities` (targetSegments, weekendFocus, peak rooms, anchors) **unchanged**
- Used only as **suggestions** when rendering (not auto-written as Hotel Confirmed)
- Airtable / local commercial profile records not migrated destructively
- Opportunity publication / 19 Bethesda cards unchanged

---

## 9. Airtable persistence

- PUT `/commercial-profile` now accepts `businessToAvoid`, `hotelConsiderations`, `knownAccounts` in addition to prior fields
- Store already mapped these into Airtable JSON columns
- Live base remains `appa2cE7FTRmIbB32` when `GDI_NEED_DATES_AIRTABLE_LIVE=1`

---

## 10. GDI supervisor integration

Verified deterministic ranking:

- Weekend + `SPORTS_GROUPS` + sports audience → `sports_entertainment` #1 with soft reasons (`hotel_weekend_sports_priority`, etc.)
- Weekday + meetings/catering → `local_catering_day_meetings` / `meetings_incentives` boosted with `hotel_weekday_meetings_priority`
- Preferences remain soft; do not invent demand or change publication eligibility

---

## 11. Screenshot

Live Tab 2 after restore: full Commercial priorities sections (revenue, weekday/weekend, group size, 14 segments, audiences, avoid/considerations, known accounts). Captured during browser verification in this session.

---

## 12. Loading performance

- No new opportunity-inventory load on Tab 2
- Keep-alive from PR #215 retained (tab switch without remount)
- Commercial form draft captured before calendar re-renders so unsaved selections survive month clicks
- Need-dates Airtable GET remains free of match upserts (PR #215)

---

## 13. Regression tests

- `test:gdi-hotel-need-times-priorities-tab-v1` — PASS (extended asserts)
- `test:gdi-hotel-toolbar-reset-view-only-v1` — PASS
- `test:gdi-qualified-customer-opportunity-feed-v1` — PASS

---

## 14. PR / merge

Filled after GitHub PR create/merge.
