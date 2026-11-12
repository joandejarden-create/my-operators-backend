# FOUNDER — Original V3 Commercial Priorities Restoration & Multi-Hotel Production Cert

**Date:** 2026-11-12  
**Branch:** `feature/gdi-hcp-v3-multi-hotel-production-cert-v1`  
**Contract:** Hotel Need Times & Commercial Priorities V3 as GDI Tab 2  
**Verdict:** `PRODUCTION_READY_WITH_LIMITATIONS`

---

## A. Original V3 recovery verification

| Item | Status |
|------|--------|
| Recovery evidence | Intact at `reports/gdi-commercial-priorities-original-uncommitted-recovery-v1/` |
| Manifest | `00-recovery-manifest.json` (branch `feat/gdi-universal-need-date-intelligence-v1`, staged-uncommitted HCP V3) |
| Preserved PR #216 reconstruction | `preserved-current/` untouched |
| Restored Tab 2 source | `public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js` — header: `ORIGINAL UI recovered` |
| Seven sections | Revenue Priorities · Need Dates · Preferred Business · Target Demand Segments · Local Market Intelligence · Hotel Fit & Constraints · Hotel Team |
| CSS | `hotel-commercial-priorities.css` + `dealality-intake-form-shell.css` linked from GDI HTML |
| Compatible PR #215/#216 | Specific Mon–Sun, excluded/lower-priority segments, tab keep-alive, sticky Save |

Evidence copies were **not** overwritten.

---

## B. Visual acceptance (real browser)

URL: `http://localhost:8080/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8` → Tab **Hotel Need Times & Priorities**

| Check | Result |
|-------|--------|
| Seven sections render | PASS |
| `hcp-chip` + `is-selected` | PASS (Group Room Nights, Weekdays, Associations) |
| Weekday/weekend + Mon–Sun | PASS |
| Fourteen demand segments | PASS (Select Specific) |
| Need-date calendar + month drawer | PASS (`hcpMonthDrawer` January 2027) |
| Hotel Team + sticky Save | PASS |
| Unsaved banner during edits | PASS |
| Desktop + mobile (390×844) | PASS |
| Wyndham Tab 2 independent | PASS — `Wyndham San Diego Bayside · 2027`, 0 active need months (not Bethesda’s 6) |

Screenshots: `reports/gdi-hcp-v3-multi-hotel-production-cert-v1/screenshots/`

---

## C. Hotel-specific behavior

### Canonical identity

Primary key = Airtable census / GDI `hotelId` (`rec…`). Selector catalog from `listGdiSelectableHotels()` — **19** eligible hotels (config + materialized data). No Bethesda hardcode in Tab 2.

### Selectable GDI hotels (full registry at cert time)

| Hotel | hotelId |
|-------|---------|
| AC Hotel A Coruña | `rec2PVBDavppGpenm` |
| Bethesda Marriott | `recLuxvwwxID7U2B8` |
| Cambridge Beaches Resort & Spa | `recIwaP1etgx2g9nA` |
| Castillo Hotel Son Vida | `rec82D9zpB8fede1I` |
| Falls Church Marriott Fairview Park | `recRkGGNKslJptFIL` |
| Hilton New York Times Square | `rec35fExUxCClpOP6` |
| Nobu Hotel London Portman Square | `recC2kbCaS48jFjFF` |
| NOW NOW NOHO | `recGkME49yYuxQl0u` |
| Radisson Hotel Santo Domingo | `recUOyzOXn2Zdp98I` |
| Renaissance New York Times Square Hotel | `recG66DQJKP2c0UNh` |
| Sheraton Mallorca Arabella Golf Hotel | `recjhQdAUiSyxfCqE` |
| Spice Island Beach Resort | `recKRJjcPnb4tVDDS` |
| The Mayflower Hotel, Autograph Collection | `rec0eTEnCqlsGJJt9` |
| The Westin Grand München | `recFaxTEFF9ILHWC9` |
| The Westin Tampa Waterside | `recdmo0CnhIZAwHGC` |
| W Rome | `rece0or38cxo3Fymb` |
| Waterstone Resort & Marina Boca Raton | `recgMYovrrZDJMqzX` |
| Wyndham San Diego Bayside | `recnPB3Q02NO8Ma1o` |
| YOTEL Geneva Lake | `recrPQcZg7SFARRb2` |

### Switching

- `selectHotel` → `clearHotelScopedState` → Tab 2 `unmount` stashes hotel-keyed draft (`_draftsByHotelId`) without silent save.
- Remount loads target hotel only; keep-alive preserves DOM **only when** `_prioritiesMountedHotelId === state.hotelId`.
- Browser: Bethesda → Wyndham showed Wyndham label; `hasBethesda: false` on Tab 2 body.

---

## D. Persistence certification

| Layer | Result |
|-------|--------|
| Store | Existing `GDI Hotel Commercial Profiles` / Need Periods via `hotel-commercial-profile-store-v1` |
| Isolation suite | `npm run test:gdi-hcp-v3-multi-hotel-isolation-v1` → **15/15 PASS** |
| Hotels exercised (LOCAL_SHADOW) | Bethesda, Wyndham SD, Waterstone, YOTEL Geneva |
| Cross-hotel contamination | None (marker notes / categories isolated) |
| Cleanup | Local cert writes restored; **no customer Airtable prefs overwritten** |
| Auth | `assertNeedDatesHotelAccess` on commercial-profile GET/PUT; cross-hotel write denied in `test:gdi-hotel-need-times-priorities-tab-v1` |
| Open Tab 2 ≠ auto-create | Opening loads profile; write only on explicit Save |

**Limitation:** Multi-hotel **live Airtable write/read-back** was intentionally not executed against production customer profiles. Path is the same store used by the portal; local-shadow isolation is certified. Safe live Airtable cert remains a follow-up with founder-approved synthetic markers.

---

## E. GDI supervisor integration

**Path:** Tab 2 Save → `saveCommercialProfile(hotelId)` →  
`resolveHotelConfigWithSavedCommercialProfile` → merges soft `commercialPriorities` (`source: HOTEL_CONFIRMED_TAB2_PROFILE`) →  
`buildHotelSegmentResearchPlan` / `buildHotelSegmentResearchPlanWithSavedProfile` →  
`runWeeklyRefreshCycle` soft-merges saved profile before `buildWeeklyWorkQueues` →  
`selectStrategiesForCampaign(..., commercialProfile)` soft boosts only.

| Guardrail | Enforced |
|-----------|----------|
| Soft signals only | Yes |
| No invent demand / Sales Ready elevation | Yes |
| No paid research from preference change | Yes (`paidExecutionBlockedUntilFounderGo`) |
| Hotel A ≠ Hotel B ranking inputs | Certified via differing weekendFocus / dayPreference on Bethesda vs Wyndham local cert profiles |

New module: `lib/group-demand-intelligence/need-date-demand/hotel-saved-commercial-profile-research-bridge-v1.js`

---

## F. Performance

| Metric | Observed |
|--------|----------|
| Warm Tab 2 keep-alive switch (Wyndham) | **31 ms** |
| Cold Tab 2 mount after hotel navigation (incl. profile load) | **~561 ms** |
| PR #215 protections | No GET-side opportunity inventory required for Tab 2; keep-alive avoids remount on same hotel |

No paid research; no opportunity inventory mutation.

---

## G. Existing opportunity protection

| Check | Result |
|-------|--------|
| Bethesda customer feed | **19/19** (`test:gdi-bethesda-67-customer-feed-reconciliation-v1`) |
| Frozen historical baseline | **67** preserved |
| Auto-publish | None |
| Paid research | None |

---

## H. Tests / GitHub

```text
npm run test:gdi-hotel-need-times-priorities-tab-v1     PASS
npm run test:gdi-hcp-v3-multi-hotel-isolation-v1       PASS (15/15)
npm run test:gdi-bethesda-67-customer-feed-reconciliation-v1  PASS
```

### Key files

- `public/js/group-demand-intelligence/hotel-need-times-priorities-tab.js` (V3 + hotel-keyed drafts)
- `public/group-demand-intelligence.html` (CSS + cache bust)
- `lib/.../hotel-saved-commercial-profile-research-bridge-v1.js`
- `lib/.../hotel-segment-research-plan-v1.js` (`buildHotelSegmentResearchPlanWithSavedProfile`)
- `lib/.../weekly-refresh-orchestrator-v1.js` (saved-profile soft merge)
- `scripts/test-gdi-hcp-v3-multi-hotel-isolation-v1.mjs`

---

## Production-readiness verdict

### `PRODUCTION_READY_WITH_LIMITATIONS`

**Ready:** Original V3 Tab 2 UI restored; browser-verified; hotel-keyed drafts; registry-driven multi-hotel UI; local multi-hotel persistence isolation; auth gate; soft supervisor bridge; Bethesda 19/67 inventory untouched; warm tab switch ~31 ms.

**Limitations:**

1. Live multi-hotel Airtable write/read-back against production tables not executed (safety — customer prefs).  
2. Learning-loop feedback → preference rewrite remains **out of scope** (preferences stay sales-controlled).  
3. Strategy soft-boost segment ID mapping still uses commercial-profile enums; continue aligning strategy IDs with fourteen research segments as research packs evolve.

**Not** `NOT_PRODUCTION_READY` — no cross-hotel isolation failure observed.
