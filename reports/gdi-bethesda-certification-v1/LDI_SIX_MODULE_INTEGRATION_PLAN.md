# Six LDI modules — canonical Full E2E integration plan

**Status:** DESIGN ONLY — no wiring, no paid discovery in this phase.  
**Hotel context:** Bethesda Marriott (`recLuxvwwxID7U2B8`) — no LDI preset today; organic sports/alumni overlap only.  
**Authority:** `lib/group-demand-intelligence/leisure-demand-intelligence/` + prior audit `reports/gdi-bethesda-gold-standard-replay-v1/14_SIX_LEISURE_*`.

## Design principles

1. LDI is a **capability layer** on the canonical 14-segment Full E2E — not a parallel engine or opportunity DB.
2. All LDI rows must pass the **same FQO six-gate contract** as association/sports/corporate rows.
3. Hotel applicability uses `selectLeisureModulesForHotel` (auto-signals + optional presets) — **not presets alone**.
4. Canonical entry must call LDI after fourteen-segment discovery and before hotel-wide finalization, with `dedupeLeisureAgainstInventory`.
5. Cost: reuse saved evidence first; paid WHO/HOW only under separate founder budget tokens.

## Integration matrix

| LDI module | Existing implementation | Canonical integration point | Research gaps | WHO/ALT/HOW | FQO pathway | Deduplication | Estimated scope |
|---|---|---|---|---|---|---|---|
| 1. Festivals / concerts / cultural (`leisure_events`) | Registry PARTIAL; PS wrapper; reuses associations + social segments; fixtures/mock live | Post-segment discovery cell → LDI runner → shared qualification | Leisure-visitor (nonlocal) evidence filters; festival lodging mechanisms | Event producer / venue partnership mgr / festival housing lead; labeled org route OK | Same FQO; demand = event + lodging thesis | Dedupe vs social_special_events + associations titles/org | M: query pack + hotel auto-select + FQO fixtures |
| 2. Participatory sports / recreation (`leisure_sports`) | OVERLAP_WRAPPER on sports_entertainment + sports housing hunter | In-path participatory filter after sports discovery | Distinguish participatory vs spectator-only lodging | Tournament housing coordinator / club director (existing Bethesda strength) | Same FQO; future-cycle for recurring cups | Strong dedupe vs existing cup inventory | S: filter + regression vs Bethesda cups |
| 3. Attractions / destination experiences (`leisure_attractions`) | PARTIAL; preferred lodging + venue partnership tracks | After preferred_lodging / travel_trade cells | Bethesda attractions partnership thin; need experience-partner queries | Attraction partnerships / group sales named person | Partnership pathway + FQO action | Dedupe vs preferred_lodging partnerships | M: query pack + WHO templates |
| 4. Universities / VFR (`leisure_universities`) | PARTIAL; education_scientific + university hunter (conference-heavy) | After education cell; specialize graduation / parents / VFR | Explicit VFR/graduation packs; alumni leisure vs conference | University housing / alumni / visitor services named person | Campaign + future pathway gates | Dedupe vs education conferences + UMD alumni watches | M: specialization prompts + fixtures |
| 5. Destination demand shifts / occasions (`leisure_destination`) | GAP — defined only; no dedicated discovery pack | New specialization over social/celebration + need-period strategy | Net-new shoulder/holiday/occasion queries | DMO / campaign / occasion organizer contacts | Campaign-driven FQO; timing critical | Dedupe vs social celebrations | L: largest net-new; design queries first |
| 6. Leisure distribution / referral (`leisure_distribution`) | OVERLAP_WRAPPER on travel trade + planner + preferred lodging | After intermediary/planner cells | Leisure-specific DMC/OTA/bureau referral framing | DMC / bureau / planner named person (Destination DC pattern) | Partnership FQO | Dedupe vs travel_trade + planner partnerships | S–M: wrapper + Bethesda DMO fixtures |

## Proposed implementation sequence

1. **Canonical hook (fail-closed OFF by default)** — enable LDI stage behind flag; Bethesda can opt-in after baseline cert.
2. **Sports participatory filter** — safest Bethesda win; regression against existing cups.
3. **Events + universities specialization** — reuse strong organic inventory patterns.
4. **Distribution / attractions** — partnership WHO templates using Destination DC / planner patterns.
5. **Destination shifts** — last; requires new query pack and timing evidence standards.
6. **Portfolio enablement** — auto-select matrix across hotels; paid research only with separate GO.

## Regression-test plan (future implementation PR)

- Unit: module registry completeness (6 modules).
- Unit: hotel auto-select without preset (Bethesda signals).
- Unit: cross-segment dedupe does not drop unique leisure rows or resurrect duplicates.
- FQO: each leisure subtype fixture passes/fails correctly (named person, future pathway, no speculation).
- Hotel-wide: LDI OFF leaves Bethesda gold 67 unchanged; LDI ON adds only FQO-pass net-new with dispositions.
- Freeze: STAGED/CURRENT checksums unchanged unless founder activates.

## Bethesda next-step (after founder approval of this plan)

Verify which modules produce **net-new** FQO-pass rows above the certified historical baseline — do not count organic sports/alumni already in gold as “LDI success.”
