# GDI 3.0 — Slice 2 Prep: Airtable Intelligence Persistence, Partners and Reuse Audit

**Status:** Founder review gate (read-only)  
**Date:** 2026-11-19  
**Workspace:** `C:\Dev\dealality\ws-gdi` · Stream `gdi`  
**Base:** AI Demand Positioning `appa2cE7FTRmIbB32`  
**Explicit non-goals:** No new tables · no schema mutations · no production writes · no backfills · no paid research · no second source of truth

**Prior evidence reused:** `reports/gdi-airtable-complete-data-model-audit-v1/*`, Full E2E persistence contract/bridge/store modules, Bethesda/Radisson frozen inventories, ORS samples.

---

## Executive verdict

GDI already has a **substantial Airtable spine** (GDO, Demand Generator graph, Private Events, Research Coverage, Need Dates, and the newer **GDI Hotel Research Opportunities** inventory). Persistence is **not missing as a platform** — it is **incomplete as a Full E2E handoff**.

The dominant failure is field loss at the **canonical chain → persistence bridge** seam: `processed[]` rows are thin summaries, so contacts (beyond name), evidence URLs, hotel-fit narrative, drawers, and partner detail do not reliably reach Airtable typed columns or even `researchPayloadJson` on the default path.

Additionally:

- Customer publication remains a **separate immutable package** (correct).
- Airtable-only reconstruction of published Bethesda cards is **NOT CERTIFIED** (prior audit).
- Partner / planner / intermediary intelligence is modeled as **Demand Generators + PE Venues + Research Targets**, not as a separate Opportunities table — which is correct — but Full E2E does not consistently write those relationships.
- Reuse exists (FS inventory + optional Airtable DG reads) but **Airtable DG/program/signal reuse is off by default** on canonical Full E2E.

**STOP** before schema changes or production writes. Prefer wiring + mapping + payload compaction over new tables.

---

## Deliverable 1 — Current Airtable GDI schema and relationship map

### Source of truth split (do not collapse)

| Layer | Store | Owns |
| --- | --- | --- |
| Airtable mutable registry | GDO + DG graph + PE + Research Coverage + Need Dates + **Hotel Research Opportunities** | Stable IDs, status, hotel-scoped inventory, operational search |
| Filesystem research working set | `data/group-demand-intelligence/hotels/{hotelId}/` (opportunities.json, ORS, runs, feedback) | Full evidence blobs, drawers, run detail |
| Global intelligence FS | `data/group-demand-intelligence/global-intelligence/` | Evidence-heavy global index (Airtable is search/ops, not blob SoT) |
| Publication | Bundled publication packages + CURRENT pointer | Immutable customer cards/drawers |
| Confidential hotel sales | `feedback.json` + sales-owned GDO columns/payload keys | Never cross-hotel; not research-owned |

Contract: `gdi-full-e2e-airtable-persistence-contract-v1.js`, `gdi-airtable-field-ownership-v1.js`, `gdi-research-persistence-contract-v1.js`.

### Tables (existing — reuse)

| Table | ID | Role |
| --- | --- | --- |
| Group Demand Opportunities (GDO) | `tblRuReslJMwsfRQj` | Hotel-scoped qualified opportunity registry |
| GDI Hotel Research Opportunities | `tblbkDj0vhQLfrssV` | Research inventory (exploratory/watchlist/continuation); identity `hotelId::opportunityId` |
| Demand Generators | `tblykUVOjGVkawvdD` | Organization / account identity (incl. planners as orgs) |
| Demand Programs | `tblxm7wqfFNG37Z2m` | Recurring programs under a generator |
| Demand Generator Signals | `tblZ0cOM78uYMCTXx` | Dated evidence/signals |
| Hotel Demand Generator Fit | `tblqamXbIk9n6gXfV` | Hotel × org qualification |
| Private Event Venues | `tblE5p4HjVHfcMDna` | Venue identity + partner flags |
| Private Event Signals | `tbl5rSZxKv218GuOP` | Event activity; planner company fields |
| Hotel Venue Fit | `tbluXqX7ecocCrdE1` | Hotel × venue partnership/catchment |
| GDI Research Targets / Runs / Target Runs | `tblVyuEf5vjooWDKX` / `tblzNMUIo2T6onaKH` / `tblSA0cFVplNfWMjp` | Monitoring + run economics |
| GDI Hotel Commercial Profiles / Need Periods / Matches / Campaign Runs | need-date suite | Hotel need windows + matches |

Relationship spine (existing):  
`Demand Generators → Programs → Signals` · `Hotel DG Fit` · `PE Venues → Hotel Venue Fit → PE Signals → GDO` · `Research Targets ↔ Runs` · `Need Periods ↔ Matches ↔ GDO`.

### Explicitly not Airtable (correct)

Hotel sales feedback, rates, outreach CRM notes, share tokens, negotiation history, raw unbounded provider dumps — see `NON_AIRTABLE_DESTINATIONS`.

### Do not create (already denied)

Second competing base; “GDI Global Demand Intelligence” table; “GDI Hotel Opportunity Matches” duplicate of Need Period Matches; Contacts table **before** typed contact columns prove insufficient (prior contact model review kept contacts embedded in GDO).

---

## Deliverable 2 — Full E2E persistence field-loss analysis

### Path traced

```
Discovery → ORS deep/contact/qualify → opportunity-research-state/*.json
  → processOpportunityThroughOriginalChain
  → chain.processed[] (thin)
  → persistence bridge extractOpportunities
  → buildResearchInventoryFields / GDO upsert mappers
  → (customer) opportunities.json / publication package / drawer DTO
```

### Loss points

| Stage | Survives | Lost / degraded |
| --- | --- | --- |
| Full opportunity in-chain + ORS file | Org, title, evidence, contacts, drawer, thesis, unresolved Qs | — |
| `processedRow` | ids, org name, segment, disposition, contact **names**, spend, selection telemetry, continuation stub | title, evidence[], sources[], email/phone/class, hotel-fit prose, drawer body, partners, stakeholders |
| Bridge default (`row` fallback) | Same as processedRow | Same — **no `opportunity` key attached** |
| Research inventory typed columns | compositeKey, lifecycle, eligibility, thesis enum, priority, title, segment, sourceSummary, provenance stamps, dgGeneratorId, researchPayloadJson | Contacts, fit, evidence counts — **JSON-only if payload present** |
| GDO typed + payload | Richer typed contact columns exist on GDO path | Payload compacted; drawer/local-only fields historically incomplete |
| Customer drawer / opportunities.json | Curated per-hotel pipelines (Bethesda/Radisson scripts) | **Not** a faithful replay of ORS→Airtable |

### Category survival (default Full E2E → research inventory)

| Intelligence category | ORS | Processed | Bridge default | Airtable typed | Customer drawer |
| --- | --- | --- | --- | --- | --- |
| Organization name / id | Yes | Partial | Partial | Partial | Yes |
| Opportunity identity | Yes | Yes | Yes | Yes (composite) | Yes |
| Evidence URLs / provenance | Yes | Mostly lost | Lost | sourceSummary often null | Rebuilt outside path |
| Named contacts (full) | Often | Name only | Name only | Research inventory: JSON-only; GDO has typed primary* | Variable |
| Partners / planners / DMCs | Segment + thesis | Lost structure | Lost | Via DG/PE routes if separately written | Thin |
| Competitor venues | Narrative / CMI fields | Lost | Lost | No dedicated competitor table; PE venue/host prose only | Variable |
| Hotel-specific fit / pursuit | Yes | Lost | Lost | GDO hotelFit/why*; research inventory weak | Variable |
| Research history / cost | Ledger + ORS | Partial | Partial | Research Runs tables exist; bridge path under-linked | Backend |
| Weekly Change | Module exists | Not wired on canonical | — | GDO change stamps; no Weekly Change table | Package/FS |

### Size trap

Passing **full** ORS objects into `researchPayloadJson` without compaction hits the **95k fail-closed** cap (`RESEARCH_PAYLOAD_TOO_LARGE`). Slice 2 must slim payloads deliberately (drop duplicate briefs / exa packs / supportingEvidenceDrawer bloat) — not silently truncate customer facts.

### Validation gap

Bethesda Airtable validation reports used injected seeds / offline plans — they do **not** prove the live chain→bridge→Airtable path preserves contacts/evidence.

---

## Deliverable 3 — Partner / contact / opportunity relationship assessment

### How partners are represented today

| Entity | Existing representation | Should become opportunity? |
| --- | --- | --- |
| Demand-generating org | Demand Generators | Only when hotel-qualified opportunity exists |
| Recurring program/event | Demand Programs + Signals | Opportunity per hotel cycle/edition when qualified |
| Meeting planner / DMC / intermediary | Demand Generator (org) + Research Target (monitor) + optional Hotel Fit | Partner is **reusable org**, not always an opportunity |
| Housing agency / planner company on an event | PE Signal planner fields / contact narrative | Prefer linked org identity over free text over time |
| Preferred lodging venue | Private Event Venues + Hotel Venue Fit | Venue relationship ≠ guest demand opportunity |
| Competitor host hotel | Prose / CMI fields / PE venue names | Preserve as evidence-linked host history; do not invent competitor table yet |

### Contact model

- **Embedded in GDO** (typed `primaryContact*`) + payload richness — intentional (contact data model review).
- Research inventory lacks typed contact columns → depends entirely on payload.
- Authority basis / lodging-owner function still weak or absent as typed fields.

### Recommendation

**Do not create Partner or Contacts tables in Slice 2.**  
Extend routing so Full E2E emits:

1. DG upsert for org / planner org  
2. Program/Signal when evidenced  
3. Hotel Fit / Venue Fit when hotel-qualified  
4. Research inventory / GDO opportunity with stable ids  
5. Typed contact columns on research inventory **or** guaranteed slim payload contact block

---

## Deliverable 4 — Existing intelligence reuse assessment

| Capability | Status | Notes |
| --- | --- | --- |
| 1. Find existing organization before re-research | **PARTIAL** | FS inventory + optional Airtable DG reuse; Airtable DG reads require `loadSharedIntelligenceFromAirtable=true` (default off) |
| 2. Identify existing market opportunity | **PARTIAL** | opportunityId / compositeKey / programKey exist; not always hydrated into bridge |
| 3. Retrieve prior evidence + verified contacts | **WEAK on default path** | ORS/FS yes; Airtable often missing full evidence/contacts |
| 4. Detect genuinely new / material change | **EXISTS** | Research inventory material-field diff; Weekly Change durability module; under-wired on canonical |
| 5. Reuse public intelligence for another hotel | **PARTIAL** | Market Opportunity Graph + cross-hotel fit modules exist; not default Full E2E intake |
| 6. Independent hotel-specific qualification | **EXISTS** | Hotel DG Fit / Hotel Venue Fit / V41 qualify — hotel-scoped |
| 7. Preserve confidential hotel sales separately | **EXISTS** | feedback.json + sales-owned field ownership |
| 8. Update existing record without duplicate | **EXISTS when identity present** | compositeKey / opportunityId upserts; thin payloads may look “new” commercially |
| 9. Track research performed and cost | **PARTIAL** | Research Runs + paid budget ledger; bridge under-links run ids |
| 10. Schedule future investigation from unresolved Qs | **PARTIAL** | Research Targets `Next Research At`; continuation packets added in Slice 1 amendment — not yet persisted to Targets |

---

## Deliverable 5 — Gap analysis (minimal changes)

| ID | Item | Classification | Proposed change |
| --- | --- | --- | --- |
| G1 | Chain `processed[]` omits full opportunity | **EXISTS_BUT_MAPPING_INCOMPLETE** | Attach `opportunityRow` slim object or ORS reload by id into bridge |
| G2 | Bridge `extractOpportunities` falls back to thin row | **EXISTS_BUT_MAPPING_INCOMPLETE** | Prefer ORS file → slim payload; never claim success with empty evidence when ORS had evidence |
| G3 | Research inventory typed contact/evidence/fit columns | **MISSING_FIELD** (typed) / **EXISTS_BUT_NOT_POPULATED** (JSON) | Add typed contact name/role/email + evidenceCount + hotelFitSummary **or** enforce slim payload contract first |
| G4 | `sourceSummary` ignores `evidence[].url` | **EXISTS_BUT_MAPPING_INCOMPLETE** | Map evidence URLs into sourceSummary |
| G5 | researchPayloadJson 95k fail-closed vs full ORS | **REQUIRES_MIGRATION** (compaction policy) | Define allowlisted slim payload; strip exa packs / duplicate briefs |
| G6 | DG/Program/Signal/Fit not written by default Full E2E bridge | **EXISTS_BUT_MAPPING_INCOMPLETE** | Extend persistence plan to route org/program/signal/fit using existing `routeResearchObjectToAirtable` |
| G7 | Planners/partners not reusable across opps | **MISSING_RELATIONSHIP** (wiring) | Always upsert DG for planner/partner orgs; link via dgGeneratorId |
| G8 | Competitor host history | **EXISTS_BUT_NOT_POPULATED** | Persist host hotel names + evidence URLs into signal/payload; defer new table |
| G9 | Airtable reuse off by default | **EXISTS_BUT_MAPPING_INCOMPLETE** | Document + optionally default-on **read-only** DG reuse behind flag for live runs |
| G10 | Research inventory ↔ GDO dual write confusion | **DUPLICATE_OR_LEGACY** risk | Keep roles: Research inventory = research SoT mirror; GDO = qualified registry; publication separate |
| G11 | Weekly Change / continuation not on Targets | **EXISTS_BUT_NOT_POPULATED** | Persist continuation stop class + next refresh into Research Targets |
| G12 | Customer drawer regeneration from Airtable | **EXISTS_BUT_NOT_POPULATED** | Out of Slice 2 critical path; package remains customer SoT until reconstruction certified |
| G13 | Cross-hotel public reuse | **EXISTS_BUT_MAPPING_INCOMPLETE** | Wire Market Opportunity Graph intake after persistence honesty |
| G14 | Silent success with empty payload | **EXISTS_BUT_MAPPING_INCOMPLETE** | Fail visible / retryable when ORS had contacts/evidence but bridge payload lacks them |

**Prefer reuse over new tables.** No Partner/Contacts/Competitor/Weekly Change tables in Slice 2 unless typed columns still fail after payload honesty.

---

## Deliverable 6 — Migration and backward-compatibility risks

| Risk | Mitigation |
| --- | --- |
| Existing GDO/research rows with thin payloads | Backfill only after dry-run diff; never overwrite sales-owned fields |
| 95k payload failures after attaching full ORS | Slim allowlist before enable; measure size distribution offline |
| Dual identity (display segment labels vs 14 workflow ids) | Normalize via legacy alias map on write; preserve raw label in payload |
| Research inventory vs GDO duplicates | Enforce compositeKey + opportunityId; eligibility status distinguishes research-only vs GDO-present |
| Publication fingerprint drift | Research persistence must not mutate publication packages |
| Confidential feedback leakage | Keep feedback FS-only; never serialize hotel feedback into shared DG rows |
| Seeded Research Targets / generic Hotel Fit | Do not treat generic_seed as verified; overwrite only with evidenced upserts |

API/write implications: production still requires `authorizeResearchPersistence` + exact write-set token + `apply` + `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1`. Paid research auth never unlocks writes.

Dedup/retry: upsert by compositeKey / opportunityId; material-change detection already in research inventory store; retries must be idempotent and surface `FAILED` visibly.

---

## Deliverable 7 — Implementation-ready Slice 2 plan

### Slice 2A — Persistence honesty (code, $0, dry-run)

1. **ORS → bridge carry:** load `opportunity-research-state/<id>.json` (or attach slim `opportunityRow`) when building bridge opportunities.  
2. **Slim payload contract:** allowlist fields (identity, evidence refs, contacts, fit summary, disposition, continuation, selection meta, run ids). Strip heavy exa packs / duplicate briefs.  
3. **sourceSummary** from evidence URLs when `sources` absent.  
4. **Fail closed visibly** if ORS had named contact/evidence and payload does not.  
5. **Dry-run fixtures** from Bethesda/Radisson/YOTEL frozen ORS + chain traces.  
6. **No production writes.**

**Acceptance tests**

- Full opportunity intelligence (identity, ≥1 evidence URL when present in ORS, primary contact name when present) survives dry-run payload.  
- Contacts/evidence not silently dropped.  
- Payload ≤ 95k for fixture corpus after slim.  
- Rejected / research-continuation rows can be planned as RESEARCH_ONLY without GDO/publication.  
- Idempotent dry-run upsert classification (CREATE/UPDATE/UNCHANGED/DUPLICATE).  
- Failure modes visible (not ok:true with empty payload).

### Slice 2B — Relationship wiring (still prefer existing tables)

1. Plan DG / Program / Signal / Hotel Fit / Venue Fit upserts via existing routers for retained opportunities.  
2. Ensure planner/partner orgs get DG identity independent of opportunity rows.  
3. Link `dgGeneratorId` on research inventory / GDO.  
4. Dry-run relationship manifests; no live schema adds.

**Acceptance tests**

- Partners reusable across opportunities via DG id.  
- Existing opportunities update rather than duplicate.  
- Cross-hotel public org identity reusable; hotel fit remains hotel-scoped.  
- Confidential feedback never appears in DG payload.

### Slice 2C — Reuse + Weekly Change hooks (after 2A/2B)

1. Enable read-only Airtable DG reuse on authorized live runs (explicit flag).  
2. Persist continuation / next refresh onto Research Targets when unresolved.  
3. Record research cost/run linkage.  
4. Still no auto-publish.

### Out of Slice 2 (founder-gated later)

- New Contacts / Partners / Competitor / Weekly Change tables  
- Production schema migrations / backfills  
- Customer package regeneration from Airtable-only  
- Paid research campaigns

---

## Founder review gate

Approve before any schema change or production write:

1. Confirm **no new tables** for Slice 2A/2B (extend existing).  
2. Approve Slice 2A persistence honesty as next implementation PR after Slice 1 (#440) merges.  
3. Decide whether typed contact/evidence columns on **GDI Hotel Research Opportunities** are required in 2A or deferred until slim JSON proves insufficient.  
4. Confirm publication remains separately authorized.

**Success for this audit:** we know exactly what already exists, what is lost on the Full E2E path, how partners should reuse existing DG/PE structures, and the minimal Slice 2 sequence to stop rediscovery waste — without inventing a second persistence platform.
