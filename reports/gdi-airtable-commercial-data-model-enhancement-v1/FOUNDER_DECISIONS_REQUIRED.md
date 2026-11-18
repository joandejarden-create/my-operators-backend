# Founder decisions required — GDI commercial data-model enhancement

**Gate:** Approve (or amend) **only** the items below before any schema column adds, GDO↔DG link activation, or production Airtable persistence that materializes commercial intelligence fields.  
**This audit is documentation-only.** It does not authorize writes.

Related prior gates (still binding if unresolved): `reports/gdi-airtable-purpose-integrity-audit-v1/FOUNDER_DECISIONS_REQUIRED.md` (D1–D8 write destinations / YOTEL–Radisson SoR).

---

## CD-1 — Weekly Change durability (no new table?)

**Context:** UI Weekly Change filter depends on `weeklyDeltaState` / `isNewThisWeek` from `weekly-delta.js`. Map already has `lastMaterialChangeAt`, `lastMaterialChangeRunId`, `firstDiscoveredAt`, `firstDiscoveredRunId`; writes are largely payload/FS today. Purpose-integrity mapping rejects a dedicated Weekly Change history table.

**Decision needed:**  
A) **Recommended** — Persist delta annotations on FS + existing GDO provenance / payload keys; **no** Weekly Change Airtable table  
B) Require typed GDO columns for `weeklyDeltaState` + `firstSeenRunId` (in addition to existing provenance map fields)  
C) Disable / hide Weekly Change filter until durable population is certified  

**Blocks:** Population work that claims customer Weekly Change is Airtable-backed; any “change ledger table” schema PR.

---

## CD-2 — Promote which presentation keys to typed GDO columns?

**Context:** Prior audits recommend typed `salesActionKey`, `commercialStageKey`, `revenueStreamClass`, `cardContactSubtitle`, `primaryContactProfessionalTitle`. All already exist as **payload / publication** keys; compaction caused Bethesda reconstruction gaps. Prefer reuse JSON unless Airtable filtering/ops need columns.

**Decision needed:** Minimum typed column set for this wave:  
A) **Recommended** — **Zero new columns**; enforce non-compaction + populate payload keys (`salesActionKey`, `salesActionPillLabel`, `commercialStageKey`, `revenueStreamClass`, `bookingWindowStatus`, CMI competitor fields, `eligibilityGaps`)  
B) Add only filter-critical typed columns: `salesActionKey`, `bookingWindowStatus` (and optionally `revenueStreamClass`)  
C) Adopt full prior audit typed set (sales + stage + revenue + contact title/subtitle)  

**Blocks:** `ensure:gdi-opportunities-airtable-schema` column adds; backfill-from-publication into typed columns.

---

## CD-3 — Activate GDO ↔ Demand Generator / Program / Signal links?

**Context:** Shared demand identity is the main structural gap. `MAP_GDI_DG_LINK` defines `dgGeneratorId`, `dgProgramId`, `dgSignalId`, `hotelGeneratorFitId` (+ link fields) but these are **not** on core `MAP_GDI_OPPORTUNITY`. DG / Programs / Signals / Fit tables already exist.

**Decision needed:**  
A) **Recommended** — Add **text id fields only** on GDO (`dgGeneratorId`, `dgProgramId`, `dgSignalId`, `hotelGeneratorFitId`); populate on qualification; Airtable link fields optional later  
B) Full Airtable linked-record fields now (Generator / Program / Signal / Fit)  
C) Defer all GDO↔DG wiring; keep `organizationName` / `organizationEntityId` only until multi-hotel reuse proves painful  

**Blocks:** Schema ensure for link/id fields; Full E2E persistence that claims shared org/program SoR via GDO.

---

## CD-4 — Demand Programs write policy (recurrence authority)

**Context:** Recurrence SoR is Demand Programs (`Recurrence Status`, `Recurrence Frequency`, cycle JSONs). Purpose mapping: create Program **only when recurring evidence confirmed**. Uncertainty remains whether near-1:1 Program↔Generator rows are meaningful series or onboard mirrors.

**Decision needed:**  
A) **Recommended** — Full E2E / weekly refresh may upsert Programs **only** when `RECURRING_CONFIRMED` or `RECURRING_HISTORICAL` with evidence; otherwise keep recurrence derived on opportunity  
B) Allow `POSSIBLE_RECURRING` Program creates for monitoring  
C) Freeze Program writes until a manual stewardship pass cleans existing near-1:1 rows  

**Blocks:** Activating Demand Programs in production Full E2E persistence routing.

---

## CD-5 — Competitor placement storage authority

**Context:** CMI fields (`competitorHotel`, `cmiCompetitorHotel`, `competitorVenueStatus`, evidence status) are payload/publication today with a strong eligibility contract. No typed GDO competitor columns.

**Decision needed:**  
A) **Recommended** — Keep CMI on `opportunityPayloadJson` + publication freeze; FS ledger for zero-CMI coverage proof  
B) Add typed `competitorHotel` + `competitorVenueStatus` on GDO for ops filtering  
C) Require a separate Competitive Intelligence table (not recommended — duplicates CMI contract)  

**Blocks:** Any competitor typed-schema PR; claims that Airtable alone can filter CMI without payload.

---

## CD-6 — Room nights & revenue estimates — what may be persisted as research fact?

**Context:** Policy explicitly marks confirmed room nights / verified volume / open RFP as **NOT_REQUIRED** for Account Development / sales-ready. GDO has `peakRooms` + `roomDemandStatus`. `estimatedRoomNights` is research JSON. Dollar revenue is **not** a research SoR; Decision Events already accept optional `roomNights` / `financialValue` / `revenueValue` on **outcomes**.

**Decision needed:**  
A) **Recommended** — Research may persist evidenced `peakRooms` / `roomDemandStatus` / optional `estimatedRoomNights` in payload with claim kind; **never** invent $ revenue on GDO; hotel-reported nights/$ only via Outcome events  
B) Add typed GDO `estimatedRoomNights` (and optionally estimate band) for filtering  
C) Also allow modeled `$` revenue on GDO when methodology is attached (higher risk — not recommended)  

**Blocks:** Schema for nights/revenue estimate columns; any eligibility change that re-introduces volume gates.

---

## CD-7 — Persist sales-readiness blocker codes on the opportunity?

**Context:** Evaluators already emit gap codes (`SALES_READY_REQUIRES`, Account Development `gaps[]`, CMI gates, actionability states). UI/commercial ops may need durable `eligibilityGaps` / `salesReadinessBlockers` without re-running divergent logic.

**Decision needed:**  
A) **Recommended** — Persist versioned gap **codes** + rule version in `opportunityPayloadJson` at research/publish time; recompute allowed with same contract version  
B) Typed multi-select / long-text column on GDO for blocker codes  
C) Compute only at serve time; do not persist (accept reconstruction drift risk)  

**Blocks:** Customer copy that asserts “blockers” from Airtable without a chosen authority.

---

## CD-8 — Production persistence activation for commercial fields (write guard)

**Context:** `AIRTABLE_PRODUCTION_WRITE_GUARD` remains fail-closed (`AIRTABLE_PRODUCTION_WRITES_NOT_AUTHORIZED`). Hotel feedback dual-write to Decisions is a **separate** certified path and must stay out of Full E2E discovery writes. Multi-hotel FS vs Airtable drift (YOTEL / Radisson / Guadalajara) may still be open under purpose-integrity D5–D6.

**Decision needed:**  
A) Authorize **dry-run-only** commercial field population mapping until CD-1–CD-7 resolved  
B) Authorize production upsert of **payload-only** commercial keys for a named hotel id list  
C) Authorize production upsert including any **new typed/link fields** approved in CD-2/CD-3  

**Blocks:** All live Airtable commercial persistence beyond already-approved feedback / prior scoped pilots.

---

## Explicitly **not** asked here (already decided or out of scope)

| Item | Why omitted |
|------|-------------|
| Create Weekly Change history table | Default reject unless CD-1 overrides |
| Create Contacts table | Contact review: defer to P2 |
| Feedback schema redesign | Decisions / Events already implemented |
| Confirmed room nights as universal sales-ready gate | Policy `NOT_REQUIRED` — only reopen if founder explicitly reverses thesis standard |
| Full E2E → Decisions / Need Periods / Commercial Profiles | Blocked under purpose-integrity D1 |
| Publication package mutation for feedback | Forbidden by package contract |

---

## Suggested approval order

1. **CD-8** scope (dry-run vs named hotel vs typed)  
2. **CD-1** Weekly Change (confirm no new table)  
3. **CD-2** typed vs payload-only presentation keys  
4. **CD-3** + **CD-4** shared identity / Programs  
5. **CD-5**–**CD-7** CMI / nights-revenue / blocker persistence  

Until CD-8 is A or a named-hotel B/C, treat commercial enhancement as **spec only**.
