# GDI Commercial Intelligence Gap Matrix

**Audit:** `gdi-commercial-data-model-enhancement-audit-v1`  
**Mode:** Read-only · prefer reuse · minimize new fields  
**Primary maps:** `lib/group-demand-intelligence/opportunity-field-map.js`, `lib/group-demand-intelligence/weekly-delta.js`, `lib/group-demand-intelligence/demand-generators/airtable-field-map.js`, `docs/decision-outcomes-airtable-fields.md`, `reports/gdi-airtable-purpose-integrity-audit-v1/PROPOSED_CANONICAL_PERSISTENCE_MAPPING.md`  
**Principle:** Not every commercial concept needs a new Airtable column. Prefer typed columns already mapped, payload JSON keys already frozen in publication, Demand Programs / DG links, FS weekly-delta / feedback stores, and Decisions / Decision Events.

## Classification legend

| Code | Meaning |
|------|---------|
| `EXISTS_STRUCTURED` | Typed Airtable column (or enum) already defined and writable via field map |
| `EXISTS_LINKED` | Relationship exists via link field / foreign id to another table |
| `EXISTS_JSON` | Lives in `opportunityPayloadJson` / generator-program payload / publication DTO only |
| `EXISTS_FILESYSTEM` | Durable on disk (hotel opportunities, weekly baseline, feedback, global-intelligence, packages) |
| `PARTIALLY_IMPLEMENTED` | Structure or code exists but population, linkage, or authority is incomplete / inconsistent |
| `GENUINELY_MISSING` | No durable typed/link/JSON/FS authority for the commercial need |
| `NOT_REQUIRED` | Explicitly out of universal requirements (policy) or wrong layer for this object |

## Priority / complexity

- **P0** — Customer filter/list/drawer fidelity or sales actionability depends on it now  
- **P1** — Commercial depth / reuse / multi-hotel identity; needed before scale persistence  
- **Complexity:** L (reuse populate/link) · M (typed column or consistent payload contract) · H (new table / graph)

---

## Gap matrix

| ID | Topic / data point | Commercial use case | Classification | Existing location (concrete names) | Recommended authority | Proposed change | Priority | Complexity |
|----|--------------------|---------------------|----------------|------------------------------------|-----------------------|-----------------|----------|------------|
| WC-01 | Weekly Change state (`NEW` / `UPDATED` / `REACTIVATED` / `UNCHANGED` / `CLOSED_DOWNGRADED` / `NOT_IN_CURRENT`) | Customer **Weekly Change** filter & tile delta pills (research freshness — not action timing) | `PARTIALLY_IMPLEMENTED` | Logic: `weekly-delta.js` → `WEEKLY_DELTA_STATE`, annotation `weeklyDeltaState`; DTO: `opportunity-list-dto.js` `weeklyDeltaState`; UI: `dealality-gdi-ui.js` Weekly Change; methodology: `reports/group-demand-intelligence/gdi-weekly-delta-methodology.md`. **Not** in `MAP_GDI_OPPORTUNITY` typed columns. | FS hotel bag + derived at weekly refresh; optional persist key in `opportunityPayloadJson` | **Reuse / populate** — ensure weekly refresh writes `weeklyDeltaState` into FS `opportunities.json` + payload; do **not** create Weekly Change history table (aligns with purpose-integrity mapping). Filter must read durable state or recompute from `firstSeenRunId` / `lastMaterialChangeAt` | P0 | L |
| WC-02 | `isNewThisWeek` | “New this week” pill / NEW filter | `PARTIALLY_IMPLEMENTED` | Derived: `deriveIsNewThisWeek(firstSeenRunId, latestCompletedWeeklyRunId)` in `weekly-delta.js`; DTO `isNewThisWeek`; contract `weekly-refresh-contract-v1.js` | Derived from `firstSeenRunId` + hotel latest weekly `runId` | **Reuse** — persist `firstSeenRunId` (see WC-03); derive boolean at serve; avoid new column | P0 | L |
| WC-03 | First-seen / material-change provenance | Stable newness & UPDATED classification across refreshes | `PARTIALLY_IMPLEMENTED` | Typed map keys exist: `MAP_GDI_OPPORTUNITY.firstDiscoveredAt`, `firstDiscoveredRunId`, `lastMaterialChangeAt`, `lastMaterialChangeRunId` — comment says “probe before select”; `opportunityToAirtableFields` currently keeps discovery provenance **payload-only**. Snapshot: `snapshotBaselineOpportunity` uses `firstSeenRunId`, `lastMaterialChangeAt`. Research Targets typed: `research-coverage/airtable-field-map.js` `lastMaterialChangeAt` | GDO typed columns **already mapped** + FS | **Populate / probe-gated write** of existing map fields; stop inventing parallel provenance columns | P0 | L |
| WC-04 | Material field change set | Explain *why* UPDATED | `EXISTS_FILESYSTEM` + `EXISTS_JSON` | `MATERIAL_FIELDS` in `weekly-delta.js`: `eventStartDate`, `eventEndDate`, `destinationStatus`, `venueStatus`, `venueSourcingStatus`, `opportunityType`, `priority`, `roomDemandStatus`, `hostHotel`, `venue`, `primaryContactName/Email/Phone` | FS weekly delta report / stage annotations | **Reuse** — keep as derived diff; optional `lastMaterialChangeSummary` in payload only if UI needs it (not P0 typed) | P1 | L |
| WC-05 | Dedicated Weekly Change history table / ledger rows per week | Audit trail of weekly deltas | `NOT_REQUIRED` | Purpose mapping: “Weekly Change history → Derived / FS · Not a dedicated Full E2E table” | FS weekly refresh outputs + existing change fields | **Do not add table** unless founder later requires CRM-grade history | — | — |
| ST-01 | Venue / hotel sourcing status | Know if lodging is open, RFP active, partially placed, fully placed | `EXISTS_STRUCTURED` | GDO: `venueStatus`, `sourcingStatus`; enums `VAL_GDI_VENUE_STATUS` (`OPEN_UNRESOLVED`, `RFP_ACTIVE_SOURCING`, `PARTIALLY_PLACED`, `FULLY_PLACED`, …); writer maps `venueSourcingStatus \|\| venueStatus` | GDO typed `venueStatus` (+ payload mirror) | **Reuse / populate** consistently; align aliases `PRIMARY_SELECTED_*` vs `PRIMARY_VENUE_SELECTED_*` | P0 | L |
| ST-02 | Demand / booking action timing (`CONTACT_NOW`, `TOO_EARLY`, …) | Distinct from Weekly Change — “when to act” filters | `PARTIALLY_IMPLEMENTED` | JSON/DTO: `bookingWindowStatus`, `bookingWindowLabel`, `bookingWindowReason`, `actionTimingFilterKey`; used by `customer-sales-action-resolver-v1.js`, `sales-opportunity-quick-filters-v1.js`, CSV export. **No** typed GDO columns in `MAP_GDI_OPPORTUNITY` | `opportunityPayloadJson.bookingWindowStatus` (+ publication freeze) | **Reuse JSON**; optionally promote `bookingWindowStatus` to typed column **only if** Airtable filtering required — else populate payload + DTO | P0 | M |
| ST-03 | Event dates (start/end) | Calendar timing for demand | `EXISTS_STRUCTURED` | GDO: `eventStartDate`, `eventEndDate` | GDO typed | **Reuse / populate**; never invent from article dates (eligibility reject list) | P0 | L |
| ST-04 | `whyNow` (commercial urgency narrative) | Sales narrative for outreach timing | `EXISTS_STRUCTURED` | GDO: `whyNow` | GDO typed + thesis standard | **Reuse / populate** as part of five-part thesis | P0 | L |
| ST-05 | Hotel commercial booking window (portal profile) | Hotel-side preferred booking window | `EXISTS_STRUCTURED` / portal-owned | Need-date / commercial profile: `bookingWindow`, `bookingWindowStrength` in hotel commercial profile store; Full E2E **blocked** from writing GDI Hotel Commercial Profiles (purpose-integrity D1) | Portal / hotel-reported profile | **Do not** invent from research; link at serve via need-date bridge only | P1 | L |
| CP-01 | Named competitor hotel | Competitive Market Intelligence (CMI) — who already has the business | `EXISTS_JSON` | Payload / pub keys: `competitorHotel`, `cmiCompetitorHotel`; contract `cmi-canonical-eligibility-contract-v1.js`; publication allowlist in `gdi-customer-publication-package-v1.js` | `opportunityPayloadJson` (+ publication freeze) | **Reuse JSON**; typed column only if founder needs Airtable filter on competitor name | P0 | L |
| CP-02 | Competitor venue / commitment status | Distinguish confirmed contracted block vs historical vs unverified | `EXISTS_JSON` | `competitorVenueStatus`, `competitorVenueStatusLabel`; `CMI_EVIDENCE_STATUS` (`CONFIRMED_COMPETITOR_COMMITMENT`, `HISTORICAL_…`, `UNVERIFIED_…`); eligibility gates | Payload + CMI contract | **Reuse / populate** via CMI classification; do not invent open-RFP spam as competitor win | P0 | L |
| CP-03 | Host / primary venue placement on *this* opportunity | Who is host hotel / venue for the cycle | `PARTIALLY_IMPLEMENTED` | Weekly material field `hostHotel`, `venue`; GDO has `venueStatus` but **no** typed `hostHotel` / `venue` columns in `MAP_GDI_OPPORTUNITY` | Payload `hostHotel` / `venue` + `venueStatus` | **Populate payload**; optional typed `hostHotel` only if filterable — prefer reuse JSON | P1 | L |
| CP-04 | Zero-CMI coverage proof | Prove competitor discovery was run even when none found | `EXISTS_FILESYSTEM` / `EXISTS_JSON` | Template CMI hunter + `CMI_DISCOVERY_COVERAGE_CHECKLIST`; reports / run ledger | FS execution ledger + opp `cmiCoverage` bag | **Reuse FS**; do not add Airtable table for coverage proof | P1 | L |
| CT-01 | Primary contact identity (name/email/phone/id) | Outreach entry point | `EXISTS_STRUCTURED` | GDO: `primaryContactId`, `primaryContactName`, `primaryContactEmail`, `primaryContactPhone`, `contactQuality`; rich object in payload `primaryContact` | GDO typed + payload | **Reuse** embedded model (contact review: table not required yet) | P0 | L |
| CT-02 | Professional title vs activity role vs card subtitle | Display accuracy; avoid remapping title→role | `PARTIALLY_IMPLEMENTED` | Typed `primaryContactRole` overloaded; JSON: `primaryContact.title`, `cardContactSubtitle`, `tileContactPreview`; audit recommends typed `primaryContactProfessionalTitle`, `cardContactSubtitle` | Prefer payload keys already in presentation persistence contract; typed only if filter/sort needed | **Populate JSON first**; add typed columns only if Airtable ops require (minimize new fields) | P0 | M |
| CT-03 | Contact class (DIRECT_BUYER / INTERMEDIARY / …) | Label intermediaries accurately; sales-ready vs entry-point | `EXISTS_JSON` | Policy enums in `COMMERCIAL_ELIGIBILITY_POLICY_V1.contactClasses`; CMI gate `VERIFIED_NAMED_CONTACT_OR_DEFENSIBLE_INTERMEDIARY`; payload contact classification fields | Payload + eligibility evaluators | **Reuse / populate** `contactClass` (or equivalent) in payload; no Contacts table | P0 | L |
| CT-04 | Intermediary / planner org as Demand Generator | Reuse planners across hotels | `EXISTS_LINKED` / `PARTIALLY_IMPLEMENTED` | DG table + Full E2E route for planner segments; Targets optional; `MAP_GDI_DG_LINK` proposes GDO↔DG links | Demand Generators (+ optional Research Target if monitoring committed) | **Link** org to DG; keep hotel-private notes off global DG | P1 | M |
| CT-05 | Normalized Contacts table | Multi-hotel contact dedupe CRM | `NOT_REQUIRED` (now) | Contact data model review: embedded model adequate; P2 only | — | **Defer**; do not create in this enhancement wave | — | — |
| CT-06 | Surfe / enrichment provider meta | Cost & request IDs | `EXISTS_FILESYSTEM` | Backend-only; scrubbed from customer copy (final-review rule) | FS usage / research ledger | **Keep non-customer**; never Airtable customer fields | P1 | L |
| ID-01 | Hotel-qualified opportunity id | Per-hotel commercial row | `EXISTS_STRUCTURED` | GDO: `opportunityId`, `hotelId` | GDO SoR for registry sync; FS `opportunities.json` for research | **Reuse** | P0 | L |
| ID-02 | Organization / demand-source identity (shared) | Reuse org intelligence across hotels | `EXISTS_STRUCTURED` + `EXISTS_FILESYSTEM` | DG: `Demand Generator ID`, `Organization Name`, `Official Domain`, …; GDO: `organizationId`, `organizationName`, `organizationEntityId`; FS `global-intelligence/` | **Demand Generators + FS global-intelligence** as shared org SoR | **Populate DG** on credible/retainGlobally; link GDO via ids (below) | P0 | L |
| ID-03 | GDO ↔ Demand Generator / Program / Signal links | Shared demand identity without duplicating orgs into every hotel | `PARTIALLY_IMPLEMENTED` | Proposed: `MAP_GDI_DG_LINK` (`dgGeneratorId`, `dgProgramId`, `dgSignalId`, `hotelGeneratorFitId`, link fields). **Not** yet in `MAP_GDI_OPPORTUNITY` core object. PE links exist: `peVenueId`, `peSignalId`, `hotelVenueFitId` | Add **id string fields** on GDO (reuse map names) + optional Airtable links | **Link / populate** `dgGeneratorId` / `dgProgramId` / `dgSignalId` — prefer text ids before heavy link UX | P0 | M |
| ID-04 | Event series / cycle identity | Match annual cycles; Weekly Change matching | `EXISTS_JSON` / derived | `deriveEventIdentity` → `eventSeriesId`, `eventCycleId`; DTO exposes `eventSeriesId`; Program: `Series ID`, `Cycle ID` on signals | Payload + Demand Programs `Series ID` when recurring | **Reuse derived keys**; persist `eventSeriesId`/`eventCycleId` in payload; link Program when confirmed | P0 | L |
| ID-05 | Out-of-market credible org without hotel GDO | Retain global intelligence without false hotel opportunity | `EXISTS_FILESYSTEM` + DG | Purpose mapping: DG only / GDO no; FS exclusion reason | DG + FS | **Reuse policy** — no new field | P1 | L |
| RN-01 | Peak rooms | Size guestroom ask | `EXISTS_STRUCTURED` | GDO: `peakRooms`; payload richer: `publishedPeakRooms`, `estimatedPeakRooms`, `peakRoomsStatus`, `peakRoomsClaimKind`, `peakRoomsEvidenceUrl`, methodology fields (`live-commercial-quality-v1.js`, overview builders) | Typed `peakRooms` + claim/status in payload | **Reuse**; never invent from attendance alone | P0 | L |
| RN-02 | Room demand status enum | Confidence band for room demand | `EXISTS_STRUCTURED` | GDO: `roomDemandStatus`; `VAL_GDI_ROOM_DEMAND` (`VERIFIED_ROOM_BLOCK`, `ESTIMATED_ROOM_DEMAND`, …) | GDO typed | **Reuse / populate** | P0 | L |
| RN-03 | Estimated / confirmed room nights (total) | Volume sizing beyond peak | `PARTIALLY_IMPLEMENTED` | JSON/research: `estimatedRoomNights`, `confirmedRoomNights` (qualification-precision / template); Decision Events optional `roomNights` for **outcomes**; policy: confirmed room nights **NOT_REQUIRED** for sales-ready | Payload estimate + Decision Event outcome nights | **Populate payload** when evidenced; do **not** make universal eligibility; optional typed GDO number only if founder wants filter | P1 | L |
| RN-04 | Attendance | Proxy for scale | `EXISTS_STRUCTURED` | GDO: `attendance`, `attendanceStatus` | GDO typed | **Reuse**; keep separate from peak rooms | P1 | L |
| RV-01 | Revenue stream class (guestrooms / F&B / combined) | Sales motion & icons | `EXISTS_JSON` | Payload/publication: `revenueStreamClass`, `revenueStreamIcons`; presentation persistence + package attach | Payload + frozen publication | **Populate / stop compacting**; typed column only if Airtable filter needed (`13-CANONICAL_SCHEMA_RECOMMENDATION.md`) | P0 | M |
| RV-02 | Modeled / estimated revenue $ | Pipeline forecasting | `GENUINELY_MISSING` as research SoR | Decision Events: optional `financialValue` / `revenueValue` for **outcomes** only; no standard GDO estimate field | Hotel-reported outcome via Decisions **or** defer | **Do not invent** research revenue estimates; capture won value on Outcome events only | P1 | L |
| RV-03 | Confirmed room nights / RFP as eligibility gates | Block sales-ready without volume | `NOT_REQUIRED` | `DEMAND_THESIS_NOT_REQUIRED` / `PROACTIVE_CI_NOT_REQUIRED`: `confirmed_room_night_demand`, `verified_room_night_volume`, `active_rfp`, … | Policy modules | **Keep NOT_REQUIRED** — absence must not block Account Development | — | — |
| RC-01 | Recurrence status / frequency | Standing vs one-time demand | `EXISTS_STRUCTURED` | Demand Programs: `Recurrence Status`, `Recurrence Frequency`; enums `RECURRENCE_STATUS` (`RECURRING_CONFIRMED`, `ONE_TIME`, …); Fit: `Recurrence Potential` | **Demand Programs** as authority for series recurrence | **Populate Programs** when series confirmed; GDO links via `dgProgramId` | P0 | L |
| RC-02 | Historical / future cycles JSON | Multi-year pattern | `EXISTS_STRUCTURED` | Programs: `Historical Cycles JSON`, `Confirmed Future Cycle JSON` | Demand Programs | **Reuse** | P1 | L |
| RC-03 | Opportunity-level one-time vs recurring disposition | Expiration / future account development | `EXISTS_JSON` / derived | `commercial-expiration-gate-v1.js` recurrence signals; actionability states; payload opportunityType `FUTURE_CYCLE` / `REACTIVATION` | Derived + `opportunityType` typed | **Reuse** enums; don’t duplicate Program recurrence onto every GDO column | P0 | L |
| SR-01 | Sales action key / pill | Customer commercial-action badge | `PARTIALLY_IMPLEMENTED` | Resolver + publication: `salesActionKey`, `salesActionPillLabel`; presentation persistence contract lists keys; Bethesda Airtable reconstruction often **missing** (67/67 gap in prior audit) | Payload + publication freeze; optional typed later | **Populate payload on upsert**; stop compaction; typed `salesActionKey` only if filter required | P0 | M |
| SR-02 | Commercial stage key | Stage / maturity filters | `PARTIALLY_IMPLEMENTED` | JSON: `commercialStageKey`; often empty in Airtable bags | Payload | **Populate** with `salesActionKey` | P0 | L |
| SR-03 | Sales-ready / Account Development eligibility gaps | Show *why not* sales-ready | `EXISTS_JSON` / computed | `SALES_READY_REQUIRES` (4 gates); `evaluateAccountDevelopmentEligibility` → `gaps[]` (`MISSING_ORGANIZATION_IDENTITY`, `DEMAND_CONFIDENCE_SPECULATIVE_OR_NEGATIVE`, …); CMI qualification gates; commercial actionability states | Computed at research/publish; persist snapshot `eligibilityGaps` / `salesReadinessBlockers` in payload if UI needs | **Reuse evaluators**; persist gap codes in payload (no new table) | P0 | L |
| SR-04 | Recommended action text | Next step for hotel sales | `EXISTS_STRUCTURED` | GDO: `recommendedAction`; also thesis `whyNow`, `hotelWinThesis`, `whyThisMatters` | GDO typed | **Reuse / populate** | P0 | L |
| SR-05 | Qualification / priority | Pipeline triage | `EXISTS_STRUCTURED` | GDO: `qualification`, `priority`, `actionStatus` | GDO typed | **Reuse**; preserve sales-owned `actionStatus`/`decisionId` on research upsert | P0 | L |
| EF-01 | Evidence freshness (`lastResearchedAt` / last verified) | Trust & stale research | `EXISTS_STRUCTURED` | GDO: `lastResearchedAt`, `researchVersion`, `sourceCount`, `sourceSummary`; payload sources often compacted history | GDO typed timestamps | **Reuse / populate** on every refresh | P0 | L |
| EF-02 | Evidence confidence score | Ranking | `EXISTS_STRUCTURED` | GDO: `evidenceConfidence` | GDO typed | **Reuse** | P1 | L |
| EF-03 | Full research history / evidence blob | Forensic redo | `EXISTS_FILESYSTEM` | FS research-stage / execution ledger; Airtable payload **compacts** history (`08-EVIDENCE_AND_RESEARCH_HISTORY_AUDIT.json`) | FS ledger SoR | **Keep FS**; do not force full history into Airtable | P1 | L |
| EF-04 | Discovery provenance fields | Which run/playbook found it | `PARTIALLY_IMPLEMENTED` | Map keys: `discoveryTargetId`, `discoveryMethod`, `discoveryPlaybook`, `discoverySource`, `runId`; writes often payload-only | GDO optional typed + payload | **Populate** existing map fields when columns live | P1 | L |
| FB-01 | Hotel Validation (response) | Customer judgment of recommendation | `EXISTS_LINKED` + `EXISTS_FILESYSTEM` | Decisions + Decision Events (`VALIDATION`); FS `share-validation.json` / `feedback.json`; GDO `decisionId`; stamps `hotelValidationUpdatedAt` | Decisions / Events SoR; FS latest projection | **Reuse** — no new feedback system | P0 | L |
| FB-02 | Sales Action (what hotel did) | Pursuit tracking | `EXISTS_LINKED` + `EXISTS_FILESYSTEM` | Decision Events `ACTION`; `salesActionUpdatedAt`; `recordAction` | Decisions / Events | **Reuse** | P0 | L |
| FB-03 | Sales Outcome (what happened) | Won / lost / booked learning | `EXISTS_LINKED` + `EXISTS_FILESYSTEM` | Decision Events `OUTCOME`; optional `roomNights`, `financialValue`/`revenueValue`; `salesOutcomeUpdatedAt` | Decisions / Events | **Reuse**; capture nights/revenue **here** when hotel reports | P0 | L |
| FB-04 | Feedback → weekly learning / research priority | Close loop into refresh | `EXISTS_FILESYSTEM` | `weekly-refresh-orchestrator-v1.js` + `hotelFeedbackToWeeklyLearningItem` / `feedbackToResearchTask`; never sync private feedback to global DG | FS feedback store | **Reuse** privacy contract | P1 | L |
| FB-05 | Feedback inside publication package | Immutable package pollution | `NOT_REQUIRED` | Policy `FEEDBACK_NOT_IN_IMMUTABLE_PACKAGE`; merge at serve | Separate merge layer | **Keep separate** | — | — |

---

## Topic roll-up (founder P0/P1)

| Founder topic | Dominant classification | Reuse verdict |
|---------------|------------------------|---------------|
| Weekly Change | `PARTIALLY_IMPLEMENTED` | Engine + map fields exist; **populate** `weeklyDeltaState` / provenance — **no new table** |
| Demand sourcing / timing | `EXISTS_STRUCTURED` + `PARTIALLY_IMPLEMENTED` | `venueStatus`/`sourcingStatus`/`event*`/`whyNow` solid; **bookingWindowStatus** stays JSON unless filtering forces typed |
| Competitor placement | `EXISTS_JSON` | CMI payload keys + contract — **populate**, typed optional |
| Contacts / intermediaries | `EXISTS_STRUCTURED` + `EXISTS_JSON` | Embedded contacts + contactClass; DG for planner orgs; **no Contacts table** |
| Shared demand identity | `PARTIALLY_IMPLEMENTED` | DG/Programs/Signals exist; **wire `MAP_GDI_DG_LINK` ids onto GDO** |
| Room nights / revenue estimates | Mixed | Peak/status structured; total nights/estimates JSON; **$ revenue not research SoR**; outcomes on Decision Events; volume **NOT_REQUIRED** for eligibility |
| Recurrence | `EXISTS_STRUCTURED` | **Demand Programs** authority |
| Sales readiness blockers | `EXISTS_JSON` / computed | Persist gap **codes** in payload from existing evaluators |
| Evidence freshness | `EXISTS_STRUCTURED` + FS history | Timestamps on GDO; history on FS |
| Hotel feedback outcomes | `EXISTS_LINKED` | Decisions / Events already certified path |

---

## Explicit non-goals (this matrix)

1. New Weekly Change Airtable history table  
2. New Contacts / OpportunityContacts graph (P2+)  
3. Research-invented dollar revenue on GDO  
4. Making confirmed room nights / open RFP universal sales-ready requirements  
5. Full E2E writes into hotel commercial profiles, need periods, or Decisions (feedback owns Decisions)  
6. Replacing immutable publication packages with live Airtable customer reads  

---

## Evidence anchors (read-only)

- `lib/group-demand-intelligence/opportunity-field-map.js`  
- `lib/group-demand-intelligence/weekly-delta.js` + `reports/group-demand-intelligence/gdi-weekly-delta-methodology.md`  
- `lib/group-demand-intelligence/demand-generators/airtable-field-map.js` + `constants.js`  
- `lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js`  
- `lib/group-demand-intelligence/gdi-proactive-commercial-intelligence-standard-v1.js`  
- `lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js`  
- `lib/group-demand-intelligence/workflow/gdi-full-e2e-airtable-persistence-contract-v1.js`  
- `docs/decision-outcomes-airtable-fields.md`  
- `reports/gdi-airtable-complete-data-model-audit-v1/{06,07,12,13,14}-*`  
- `reports/gdi-airtable-purpose-integrity-audit-v1/PROPOSED_CANONICAL_PERSISTENCE_MAPPING.md`  
- `reports/gdi-hotel-feedback-airtable-persistence-v1/FOUNDER-HOTEL-FEEDBACK-PERSISTENCE-REPORT.md`
