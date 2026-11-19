# GDI Discovery Quality Pivot Plan V1

**Status:** Plan (post Phase 2B STOP)  
**Date:** 2026-11-19  
**Parent evidence:** PR #411 / #412 / #413 · fourteen-segment audits · Bethesda Exa effectiveness · Full E2E executor registry  
**Explicit non-goals:** No CMAA/DDAA deepen · no parallel research platform · no paid providers without separate founder auth

---

## Why this pivot

Phase 2B proved that **more official-domain URL loops on already-known opportunities** do not create commercial value when:

- lodging demand is unpublished (`TOO_EARLY`), or
- the primary event is already competitor-placed, or
- public pages do not expose extractable named buyers.

GDI development continues by improving **how we discover new forward-looking demand**, not by re-researching the same two cases.

---

## Phase 1 — Discovery architecture assessment

### Canonical 14 segments

Source: `FOURTEEN_SEGMENTS` / `FOURTEEN_SEGMENT_EXECUTOR_SPECS`

| # | Segment ID | Discovery strategy (configured) | Specialized runner | Providers available |
|---|------------|----------------------------------|--------------------|---------------------|
| 1 | `associations_conferences_events` | assoc strategies + segment executor | association-events hotel runner | Exa (paid), historical ledger, fixture |
| 2 | `corporate_accounts_scaling` | scaling strategies + segment executor | scaling-companies hotel runner | Exa, historical, fixture |
| 3 | `corporate_meetings_incentives` | destination corporate demand strategies | shared executor | Exa, historical, fixture |
| 4 | `government_public_sector` | government manual + GDI queries | shared | Exa, historical, fixture |
| 5 | `sports_entertainment` | base-of-demand sports | shared | Exa; Apify family preferred but mock/fail-closed by default |
| 6 | `education_scientific` | medical assoc + pharma base | shared | Exa, historical, fixture |
| 7 | `projects_infrastructure` | project workforce base | shared | Exa, historical, fixture |
| 8 | `social_special_events` | venue discovery queries | shared | Exa; Apify family |
| 9 | `local_catering_day_meetings` | local meetings/catering queries | shared (+ LMC workflow) | Exa, historical, fixture |
| 10 | `corporate_celebrations_private_events` | destination recognition/celebration | shared | Exa, historical, fixture |
| 11 | `travel_trade_group_intermediaries` | professional planner queries | shared | Exa, historical, fixture |
| 12 | `relocation_extended_stay` | relocation templates | shared | Exa, historical, fixture |
| 13 | `preferred_lodging_partnerships` | preferred lodging prompt builder | shared | Exa, fixture |
| 14 | `professional_event_planners` | planner prompt builder | shared | Exa, fixture |

**Classification (all 14):** `PARTIAL_EXECUTOR` — shared `runSegmentDiscoveryExecutor` always present; live paid discovery requires `authorizePaidDiscovery=true`.

### Seeded URLs vs genuine source discovery

| Mode | What happens | Claims live discovery? |
|------|----------------|------------------------|
| Fixture provider | Deterministic fake hits | **No** |
| Historical ledger provider | Replays prior Exa/inventory rows | **No** (`HISTORICAL_REPLAY`) |
| Zero-cost blocked | No provider; awaits auth | **No** |
| Paid Exa agent | Query → web hits | **Yes** (when authorized) |
| Official-page deepen (Phase 2/2B) | Same-domain fetch from known seeds | Retrieval, **not** org discovery |

**Finding:** Without paid discovery auth, Full E2E “discovery” for most segments is **query construction + replay/fixture**, not genuine source discovery. Inventory richness (e.g. YOTEL multi-segment packages) proves **qualification/publication coverage**, not that each segment ran live multi-provider discovery (fourteen-segment execution audit).

### Who has demonstrated new verified opportunity discovery

Evidence: Bethesda Exa live effectiveness (`FOURTEEN_SEGMENT_EFFECTIVENESS.md`) under a bounded paid campaign:

| Theme | Result |
|-------|--------|
| Best new unique | `corporate_accounts_scaling` (e.g. Amentum watchlist), `government_public_sector`, `social_special_events` |
| High reject / wrong catchment | Associations (NASPA/SIOP), corporate meetings (national AEA/AHA), sports, education calendars |
| Duplicate without pre-deep dedupe | Preferred lodging (Rock Creek Mansion) |
| Weak forward-looking framing | Projects, relocation, local catering in that sample |

Offline Bethesda scorecard (paid Exa $0): **Fresh discovery hits ≈ 0–2 per segment**; most “inventory” is already-mapped FS bag, not new discovery.

### Where research becomes repetitive / non-actionable

1. **Event-calendar gravity** — queries over-index on published national conferences vs local rotational / overflow / pre-RFP signals.  
2. **Seed-URL deepen loops** (Phase 2B) — more pages ≠ new orgs, lodging proof, or named buyers.  
3. **Re-processing published inventory** without org-level cross-hotel reuse / competitor-placed state.  
4. **Generic write-ups** — scorecard repeatedly flags “replace generic hotel-fit boilerplate with org-specific thesis.”  
5. **Contact orphaning** — public emails without name+role pairs; buyer intelligence cannot invent names.

### How entities connect today (partial graph)

Existing building blocks (must reuse, not rebuild):

| Concern | Existing module / gate |
|---------|------------------------|
| Supervisor | ORS `evaluateOpportunity` |
| Commercial eligibility | `evaluateSharedCommercialEligibility` |
| Recurring / competitor host | `recurring-meeting-intelligence-v1` (`COMPETITIVE_OVERFLOW`, `FUTURE_CYCLE_COMPETITIVE`, …) |
| Cross-hotel fit | `market-opportunity-graph/cross-hotel-fit-v1` |
| Weekly refresh / skip unchanged | `gdi-organization-investigation-v1`, weekly refresh orchestrator |
| Write-up fields | `whyActNow`, `recommendedNextStep` (certified drawer contract) |
| Dedup / identity | nextgen discovery dedupe, Parallel identity dedupe, canonical entity keys |
| Persistence / publication | Airtable routing + customer publication gates (unchanged) |

**Gap:** Discovery executors do not consistently **emit and consume** a durable org→opportunity→evidence→contact→hotel-state graph before spending retrieval budget. Recurring/competitor classifiers exist but are under-applied at discovery intake.

---

## Phase 2 — Prioritized improvements (mapped to product goals)

| Goal | Current weakness | Direction |
|------|------------------|-----------|
| 1. Forward-looking demand before RFPs | Calendar/Exa event bias; projects/relocation thin | Segment missions for **announced projects, workforce deployments, multi-year programs, rotational city shortlists** — not only dated event listings |
| 2. Predict recurring demand | Recurring intel exists offline | Wire program keys + cycle watch into discovery intake and revisit queues |
| 3. Named stakeholders / referral routes | Orphan emails; weak public pairing | Contact pairing only from visible name+title+mailto; org routes labeled separately from buyers |
| 4. Timing + revisit | TOO_EARLY stuck until manual re-run | Evidence-triggered revisit (housing page published, venue RFP open) |
| 5. Open vs competitor-placed vs speculative | Frozen fields exist; not enforced at intake | Hard dispositions at discovery: `OPEN_OR_UNKNOWN`, `COMPETITOR_PLACED`, `SPECULATIVE_WATCH` |
| 6. Cross-hotel reuse | Cross-hotel fit shadow only | Org/program-level cache: research once, score hotel fit N times |
| 7. Sales write-ups | Generic boilerplate | Enforce certified drawer: org-specific thesis + whyActNow + next step or fail gate |

---

## Three highest-impact discovery bottlenecks

### Bottleneck 1 — Discovery yield is replay-shaped unless paid Exa is authorized

**Evidence:** Executor registry `zeroCostMode: fixture_or_historical_or_blocked_paid`; Bethesda $0 scorecard fresh≈0; historical ledgers often `UNKNOWN` per segment.

**Architectural change:** Introduce a **Discovery Yield Contract** on `runSegmentDiscoveryExecutor` outputs:

- Label every candidate: `LIVE_NEW` | `HISTORICAL_REPLAY` | `FIXTURE` | `INVENTORY_REOPEN`
- Fail closed: never mark `DISCOVERY_COMPLETED` from fixture/historical alone (already partial — harden + surface in hotel run reports)
- Add **$0 public discovery paths** where safe (official calendars / gov portals / sports sanction bodies via official_page allowlists) as first-class providers beside Exa — not seed-URL deepen of known opps

### Bottleneck 2 — Missions optimize for published events, not open hotel demand

**Evidence:** Bethesda Exa effectiveness — national associations EXCLUDE; preferred lodging DUPLICATE; Phase 2B competitor-placed CMAA banquet; DDAA lodging unpublished.

**Architectural change:** Per-segment **Demand Mechanism Mission Pack** (reuse `demand-mechanism-research-v1` + recurring intel):

- Require queries to target at least one of: open sourcing language, overflow/housing need, project mobilization, rotational shortlist, preferred-vendor RFP
- At intake, classify: `OPEN` / `COMPETITOR_PLACED` / `TOO_EARLY_WATCH` / `SPECULATIVE`
- Drop or park `COMPETITOR_PLACED` primary blocks from sales-ready paths (keep overflow only when evidenced)

### Bottleneck 3 — No intake-time graph reuse → duplicate research and weak actionability

**Evidence:** Preferred lodging duplicate; Phase 2 experiments re-scored known IDs; write-up generics in scorecard; contact extraction fails without graph of people↔roles↔pages.

**Architectural change:** **Market Opportunity Graph intake gate** (reuse existing graph modules):

- Before deep/public fetch: resolve org+program key; attach prior evidence, competitor host, cross-hotel applicability
- Emit sales packet only if `whyActNow` + `recommendedNextStep` pass certified drawer contract
- Cross-hotel: one org investigation → N hotel fit evaluations

---

## Phase 3 — Controlled validation design (bounded, representative)

**Do not** re-open CMAA/DDAA deepen.

### Design

| Item | Spec |
|------|------|
| Hotels | 2 registered GDI hotels with rich inventories (e.g. Bethesda + one non-US) — **read-only / offline first** |
| Segments | 5 priority: `corporate_accounts_scaling`, `projects_infrastructure`, `associations_conferences_events`, `government_public_sector`, `education_scientific` |
| Providers (slice 1) | **$0 only**: historical ledger + inventory bag + official_page allowlist optional; **no Exa** until separate auth |
| Metrics (commercial, not rubric theater) | Count of `LIVE_NEW` or newly promoted `TOO_EARLY→OPEN` with provenance; new verified named buyers; competitor-placed correctly parked; cross-hotel reuse hits; write-ups passing drawer contract |
| Explicit non-metrics | Extra URLs fetched; narrative length; recovery-overlay score deltas |

### Distinction checklist (per candidate)

- [ ] Genuinely new organization  
- [ ] Genuinely new opportunity (program/cycle)  
- [ ] Newly verified demand evidence  
- [ ] Newly verified commercially relevant contact  
- [ ] Qualification change (and correctness)  
- [ ] Already booked elsewhere  
- [ ] Hotel-specific actionability (`whyActNow` + next step)

Preserve: ORS, commercial eligibility, dedupe, provenance, persistence, customer-publication gates.

---

## First testable implementation slice (recommended)

### Slice name: `gdi-discovery-intake-disposition-v1`

**Scope (narrow):**

1. **Intake disposition classifier** wrapping discovery/historical candidates using existing `recurring-meeting-intelligence-v1` + commercial eligibility signals → `OPEN_OR_UNKNOWN` | `COMPETITOR_PLACED` | `TOO_EARLY_WATCH` | `SPECULATIVE_WATCH` | `INVENTORY_REPLAY`.  
2. **Yield labeling** on segment executor outputs (`LIVE_NEW` vs `HISTORICAL_REPLAY` vs `FIXTURE` vs `INVENTORY_REOPEN`) — no new providers.  
3. **Offline validation harness** on frozen Bethesda (+ optional YOTEL) inventories: measure % mislabeled competitor-placed / replay presented as fresh; measure write-up contract pass rate on a 20-opportunity sample **excluding** CMAA/DDAA Phase 2B IDs.  
4. Unit tests only; `$0`; no Airtable writes.

**Success for slice 1:**

- ≥80% of known competitor-placed / NOT_REQUIRED banquet-style rows classified `COMPETITOR_PLACED` or non-guestroom appropriately on the frozen sample  
- 0 fixture/historical candidates labeled `LIVE_NEW`  
- Drawer contract failures enumerated with field-level gaps (thesis / whyActNow / next step)

**Explicitly deferred:** paid Exa mission upgrades; JS board contact pairing; multi-hotel production graph writes.

### Then (slice 2+, founder-gated)

- Catchment-local mission packs for scaling + projects (still can dry-run query diffs `$0`)  
- Optional bounded Exa only after slice 1 gates pass and founder authorizes budget  
- Cross-hotel org cache persistence

---

## Recommendation summary

| Priority | Bottleneck | Change | First proof |
|----------|------------|--------|-------------|
| **P0** | Replay disguised as discovery | Yield labels + intake disposition | Offline harness on frozen inventories |
| **P1** | Event-calendar / wrong-demand missions | Demand-mechanism mission packs + OPEN vs competitor vs TOO_EARLY | Query/mission unit tests + disposition accuracy |
| **P2** | No graph reuse / weak sales packets | Graph intake + certified write-up gate | Drawer contract audit on sample |

**Do not** expand Phase 2B. **Do** implement slice 1 next as a focused PR under standard CI → Integration Controller merge → `ws/gdi` restore.
