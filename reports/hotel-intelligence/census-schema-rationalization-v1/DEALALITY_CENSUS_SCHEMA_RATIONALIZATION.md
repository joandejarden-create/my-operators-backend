# Census Schema Rationalization V1

**Status:** `CENSUS_SCHEMA_RATIONALIZATION_V1_COMPLETE`  
**Date:** 2026-08-12  
**Mode:** READ-ONLY audit + founder-review manifest  
**Tripadvisor Tier A writes:** **PAUSED**  
**Production schema changes executed:** **0**  
**Production data writes:** **0**

## North star

```text
LEAN HOTEL CENSUS          → WHAT hotel is this? durable core attributes
RICH HOTEL INTELLIGENCE    → observations, profiles, comparisons
DEEP EVIDENCE/PROVENANCE   → source-level lineage
EXTERNAL IDS               → provider identifiers
OWNER INTELLIGENCE         → ownership / operator graphs
```

Do **not** grow Airtable every time a provider (Tripadvisor, GIATA, HBX, …) is added.

---

## 1. Current 146-field inventory

Live table: **Hotel Property Census** (`tbl9aY5ijiuIzzWam`) — **146 fields**.

Artifact: `01-current-field-inventory.json`  
Schema snapshot: `data/hotel-intelligence/census-schema-rationalization-v1/schema-before.json`

Audit script: `scripts/census-schema-rationalization-audit.mjs` (read-only).

---

## 2. Population / completeness by field

Census records scanned: **15,485**.

Artifact: `02-population-completeness.json`

### High-value gaps (core)

| Field | Completeness |
|-------|--------------|
| Latitude / Longitude | ~5.4% |
| Rooms / Keys | ~1.2% |
| Hotel Class / Segment | **0%** (empty — keep for future) |
| Property Type | ~2.3% |
| Official Property URL | ~51.9% |
| Address | ~63.3% |
| Phone | ~80% |
| Current Brand | ~13.6% |

### Empty fields (43)

Includes entire **Owner / Operator / Developer** suites, many **Possible*** opportunity flags, amenity boolean flags, Opening/Renovation suite, Hotel Class / Segment, Short Property Summary, etc.

See inventory matrix for full list.

### Shell / candidate pipeline (heavily populated, deprecate)

| Field cluster | Populated |
|---------------|-----------|
| Shell Insert * / Discovery Source / Source Candidate * | ~12,922 |
| Candidate Brand Source / Confidence / Brand Validation | ~1,908 |

---

## 3. Dependency audit

| Scope | Status |
|-------|--------|
| Repo search (`lib`, `api`, `scripts`, `docs`, `public`, `airtable`) | **COMPLETE** (6,109 files) |
| Field name + field ID substring matches | Recorded per field |
| Airtable views / interfaces / automations | **UNKNOWN_NOT_QUERIED** |
| Airtable formula field dependencies | Spot-check via schema options only |

Artifact: `03-dependency-audit.json`

**Rule:** UNKNOWN Airtable dependency ⇒ **DO NOT DELETE**.

Note: short names (`Phone`, `City`, `Country`) over-count in substring search; use `FIELD_ID` hits + mapping constants for higher confidence.

---

## 4. Field disposition matrix

Artifact: `04-field-disposition-matrix.json`

| Disposition | Count |
|-------------|------:|
| KEEP_CORE | 22 |
| KEEP_SUPPORTING | 55 |
| REPURPOSE | 0 |
| CONSOLIDATE | 3 |
| MOVE_OUT (HI / Evidence / External IDs / Owner) | 53 |
| DEPRECATE | 12 |
| DELETE_CANDIDATE | 0 |
| UNKNOWN_REQUIRES_REVIEW | 1 (`Brand-Unassigned Reason`) |

**MOVE_OUT breakdown:** HI 28 · Evidence 8 · External IDs 2 · Owner Intelligence 15

---

## 5. Duplicate / redundant fields

Artifact: `05-duplicates-redundant.json`

| Field | Action |
|-------|--------|
| Market / Submarket | **CONSOLIDATE** into `Market` + `Submarket` |
| Continent | **CONSOLIDATE** / derive from Country config |
| Sub-Continent | **CONSOLIDATE** / derive from Country config |

No Tripadvisor/GIATA parallel columns found on census (good).

---

## 6. Provider-specific fields

Artifact: `06-provider-specific-fields.json`

| Cluster | Disposition |
|---------|-------------|
| HBX Hotel Code, HBX Chain Code | **MOVE_TO_EXTERNAL_IDS** |
| Other HBX * metadata | **MOVE_TO_EVIDENCE_STORE** then deprecate columns |
| tripadvisor_* / giata_* / google_* census columns | **None present** — keep it that way |

---

## 7. Fields suitable for repurposing

Artifact: `07-repurpose-candidates.json`

**REPURPOSE count: 0**

Rationale: required Tripadvisor destinations already exist (`Official Property URL`, `Phone`, `Address`, Lat/Lng, `Hotel Class / Segment`, `Property Type`, `Rooms / Keys`).  

**Do not** invent Email via rename during cleanup — store Email in evidence/HI until lean schema is approved; only then consider Phone-parity Email suite.

---

## 8. Fields to move outside Census

Artifact: `08-move-outside-census.json`

| Destination | Examples |
|-------------|----------|
| Hotel Intelligence | Amenity flags, Possible* opportunity flags, long descriptions/AI summaries, Candidate Brand Text |
| Evidence store | HBX detail metadata, source URLs for renovation/owner (long-term), raw amenity text |
| External IDs | HBX Hotel Code / Chain Code |
| Owner Intelligence | Owner*, Developer*, Operator*, Management Model |

---

## 9. Delete candidates

Strict `DELETE_CANDIDATE` (empty + zero repo refs): **0**

**Practical Bucket A (33)** = empty MOVE/DEPRECATE fields that are *candidates* for hide/delete **after** Airtable view/automation verification:

Artifact: `12-execution-buckets.json` → `BUCKET_A_SAFE_NOW`

Examples: empty Owner/Operator/Developer suite, empty amenity flags, empty HBX detail columns, empty Opening/Renovation companions (Opening Date itself is KEEP_CORE even when empty).

---

## 10. Ideal lean Census schema

Artifact: `10-ideal-lean-census-schema.json`

Designed for Dealality **today**:

- **Identity:** Property Identity Key, Canonical Property Name, Property Name, Production Use Status, Identity Confidence  
- **Location:** Address, City, State/Region, Country, Market, Submarket, Lat, Lng  
- **Property:** Rooms/Keys, Property Type, Hotel Class/Segment, Opening Date  
- **Affiliation:** Current Brand, Brand Family, Affiliation Status  
- **Contact:** Official Property URL, Phone  
- **Provenance min:** record-level Source* + Rooms/Address/Coordinate/Phone confidence companions  
- **Workflow min:** Enrichment/Review/Data Eligible/Family  
- **Links:** Brand Affiliations / Source Evidence / Steward Review  

**Target:** ~**55** fields (stretch ceiling **65**).  
**Explicitly out:** provider observation columns, owner graphs, opportunity flags, narratives, rankings/ratings, shell-batch metadata, duplicate geography.

---

## 11. Current vs target

| Metric | Value |
|--------|------:|
| CURRENT_FIELDS | 146 |
| TARGET_FIELDS | 55 |
| FIELD_REDUCTION_COUNT | 91 |
| FIELD_REDUCTION_PERCENT | **62.3%** |

Path: KEEP (~77 core+supporting today) → migrate MOVE_OUT/DEPRECATE → consolidate 3 → optional Wave 4 provenance thinning toward 55.

---

## 12. Airtable burden analysis

| Burden class | Fields / notes |
|--------------|----------------|
| Long text | Hotel Description*, Amenities - Source Text, Notes for Steward, Candidate Brand Text, Building/Asset Notes |
| Many booleans | Fitness/Pool/Parking/Spa/Beach/Meeting/Resort/Extended Stay/… |
| Provider column suite | 10 HBX fields |
| Shell ingest metadata | 8+ highly populated operational columns |
| Opportunity flags | Possible* (mostly empty) |
| Duplicate geo | Market/Submarket combo + Continent/Sub-Continent |
| Provenance companions | Necessary short-term; expensive long-term if multiplied per provider |

**Expensive pattern to ban:** `provider_field` + `provider_field_source` + `provider_field_confidence` × N providers.

---

## 13. Migration requirements

| Wave | Work |
|------|------|
| Before any delete | Export `(record_id, field_id, field_name, type, value)` for populated affected fields |
| HBX codes | Upsert into local `external-ids` registry; update HBX linkage scripts |
| Owner/Operator | Move model to Owner Intelligence tables/store; freeze census columns |
| Narratives / flags | Persist last values into HI/evidence JSON if non-empty |
| Shell metadata | Archive batch CSV/JSON then deprecate columns |
| Market/Submarket combo | Parse → Market + Submarket; verify no unique values lost |
| Continent* | Derive from country config; validate against stored values before drop |

Artifact: `15-schema-rationalization-manifest.json`

---

## 14. Rollback strategy

1. `schema-before.json` saved under `data/.../census-schema-rationalization-v1/`  
2. Before Bucket B: full field-value export for affected fields (`data-before-export` — **pending per wave**)  
3. No live deletes until founder approval + Airtable dependency confirmation  
4. Recreate fields from backup + re-import values if needed  
5. Keep Tripadvisor writes paused until retained destinations confirmed  

**BACKUP_STATUS:** schema snapshot saved; **per-field data export pending** before destructive waves.

---

## 15. Schema rationalization manifest

Artifact: `15-schema-rationalization-manifest.json`

Every field has: OLD_FIELD, ACTION, NEW_FIELD/DESTINATION, POPULATED_RECORDS, REPO deps, migration required, risk, rationale.

**Status:** `AWAITING_FOUNDER_REVIEW`  
**STOP** before Bucket B destructive changes.

---

## 16. Tripadvisor destination mapping (after cleanup)

Artifact: `16-tripadvisor-destination-mapping.json`  
**Tripadvisor Tier A remains PAUSED.**

| TA concept | Tier | Destination after rationalization | New column? |
|------------|------|-------------------------------------|-------------|
| Official Property URL | A | Official Property URL | No |
| Phone | A | Phone (+ confidence/source companions) | No |
| Address | A | Address | No |
| Latitude / Longitude | A | Latitude / Longitude | No |
| Hotel Class / Segment | B | Hotel Class / Segment (KEEP_CORE even if empty) | No |
| Property Type | B | Property Type | No |
| City / State | B | City / State / Region | No |
| Amenities tags | B | Amenities - Structured Tags (or HI if moved) | No |
| Email | B | Evidence/HI only — **do not add** during cleanup | No |
| Rooms | C | Authoritative `Rooms / Keys` unchanged; TA → evidence candidate | No |
| Rating/rank/price/awards | HI | Profile Pack + evidence | No |

**TRIPADVISOR_FIELDS_USING_EXISTING_COLUMNS:** 12  
**TRIPADVISOR_FIELDS_REQUIRING_NEW_COLUMNS:** **0**

---

## 17. Recommended execution waves

| Wave | Action | Writes? |
|------|--------|---------|
| **0** | Founder review this manifest | No |
| **1** | Airtable UI: list views/interfaces/automations touching Bucket A empties | No |
| **2** | Export backups; migrate Bucket B (HBX→external-ids, Owner→OI, narratives→HI) | Schema deprecate after migrate |
| **3** | Consolidate Market/Submarket + Continent* | Controlled |
| **4** | Optional: thin provenance companions once evidence-store parity proven | Controlled |
| **5** | Re-count schema; re-run completeness; validate BE/HI/MCP/mappings | No TA yet |
| **6** | Resume **Tripadvisor Tier A** null-fills into **retained** columns only | Tier A only, gated flags |

---

## Rooms / Keys rationalization

Artifact: `07b-rooms-keys-rationalization.json`

| Role | Field |
|------|-------|
| Authoritative | `Rooms / Keys` (191 populated) |
| Provenance companions (KEEP_SUPPORTING) | Confidence, Source Type/URL, Reviewed Date, Evidence Tier, Notes |
| Candidates (Tripadvisor, etc.) | **Evidence store only** — never new provider room columns |

**NO room values may be lost** during cleanup.

---

## Change impact

**High** (schema design) — but **zero** production schema/data mutations in this phase.

## Definition of done (this phase)

- [x] Full 146-field inventory + population  
- [x] Repo dependency audit (Airtable views still UNKNOWN)  
- [x] Disposition + lean target + buckets + manifest  
- [x] Tripadvisor remap with **0 new columns**  
- [x] Tripadvisor writes remain paused  
- [ ] Founder approval for Wave 1+  

## Manual QA (founder)

1. Open `15-schema-rationalization-manifest.json` and review MOVE_OUT / DEPRECATE lists  
2. In Airtable, check views/automations for Bucket A empty fields  
3. Approve Wave 1 scope (or request disposition overrides)  
4. Do **not** enable Tripadvisor write flags until Wave 5–6
