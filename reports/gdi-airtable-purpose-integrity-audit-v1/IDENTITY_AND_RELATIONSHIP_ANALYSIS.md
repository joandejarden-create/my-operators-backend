# Identity and relationship analysis

## Concept boundaries (Phase C)

### Demand Generators vs Group Demand Opportunities

| | Demand Generators | Group Demand Opportunities |
|--|-------------------|----------------------------|
| Object | Organization / demand source (reusable) | Hotel-specific qualified opportunity |
| Identity | `Demand Generator ID` | `opportunityId` (+ `hotelId`) |
| When to create | Credible org / retainGlobally | Hotel-qualified or watchlist opportunity for **this** hotel |
| Anti-pattern | Creating a GDO for every global org | Using GDO as global company master |

### Demand Programs vs Demand Generator Signals

| | Programs | Signals |
|--|----------|---------|
| Object | Recurring series / standing program | Dated occurrence / monitoring hypothesis / event signal |
| Population today | 94 (near 1:1 with generators) | 23 (sparse, Bethesda-heavy) |
| Full E2E | Only with recurring evidence | Only with dated signal evidence — not raw hits |

### GDI Research Targets vs Demand Generators

| | Research Targets | Demand Generators |
|--|------------------|-------------------|
| Role | **Monitoring queue / research subject** for a hotel | **Persistent org intelligence** |
| Live reality | Many `generic_seed` reasons; 480 rows | 95 orgs; some onboard-seed provenance |
| Full E2E | Add targets when committing to monitor — not automatic for every discovery | Upsert org facts independently |

### GDI Research Runs vs Full E2E Execution Ledger

| | Research Runs (Airtable) | Execution ledger (FS) |
|--|--------------------------|------------------------|
| Role | Lightweight run summary / coverage UI | Immutable provider/cache/fixture execution truth |
| Count | 40 | Per-hotel run JSON under observability paths |
| Rule | Link `Run ID` ↔ ledger `runId`; **do not** dump Exa rows into Airtable |

### Private Event Venues vs Signals vs Hotel Venue Fit

1. **Venue** = durable place identity  
2. **Signal** = activity / event evidence at venue  
3. **Hotel Venue Fit** = hotel-specific partnership / catchment assessment  
4. Qualified PE may also create/link a **GDO**

### GDI Hotel Commercial Profiles vs Hotel Commercial Profiles

| | GDI Hotel Commercial Profiles | Hotel Commercial Profiles |
|--|------------------------------|---------------------------|
| Purpose | Portal commercial priorities / accounts / restrictions | Facility physical/product attributes |
| Count | 2 | 23 |
| Authority for need-date | **GDI table** | Link only — never override GDI priorities |
| Writer | `hotel-commercial-profile-store-v1.js` (explicitly avoids mutating facility) | `hi-airtable-store.applyHotelIntelligencePacket` |

### Decisions vs Decision Events vs opportunity status

| | Decisions | Decision Events | Opportunity fields / FS feedback |
|--|-----------|-----------------|----------------------------------|
| Role | Decision record / recommendation state | Append-only lifecycle events | Local feedback + status on opportunity |
| Merge | Read path merges Airtable + `feedback.json` | | |

## Relationship health

Documented links (from prior relationship map + live meta) remain correct in schema:

`DG → Programs → Signals → Fit → GDO`  
`Venue → Venue Fit → Signals → GDO`  
`Need Period → Matches → GDO`  
`Targets → Target Runs → Runs`

**Population gaps:** many link fields empty on seed-era rows. That is incomplete usage, not an incorrect model.

## Identity hygiene recommendations (no migration in this PR)

1. Treat **Match Key** as Match table upsert identity in contracts (not Need Period ID).  
2. Keep `opportunityId` hotel-scoped uniqueness.  
3. Never reuse another hotel’s opportunity IDs when switching hotels (UI already enforced in later GDI PRs).
