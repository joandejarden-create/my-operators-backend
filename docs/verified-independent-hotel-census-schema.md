# Verified Independent Hotel Census — Final Schema Proposal (Phase 2H)

**Status:** Documentation only. **No Airtable fields created.** No ingest, no promotion scripts, no production wiring.

**Related docs:** [independent-hotel-census-pipeline.md](./independent-hotel-census-pipeline.md), [independent-hotel-census-inventory.md](./independent-hotel-census-inventory.md)

**Table name (current):** `Verified Independent Hotel Census` (`tbljBvId1z4cEyE1J` in Platform base)

**Future rename (not now):** When production-ready, this table may be renamed to **Verified Hotel Master** or **Dealality Hotel Master**. Rename is a separate migration decision after schema stabilization and explicit approval.

---

## 1. Census layer roles

| Layer | Table | Role |
|-------|--------|------|
| **Legacy / reference** | `Hotel Census` | STR-backed production census (~15,635 rows). **Read-only** for independent pipeline: duplicate detection, coverage gaps, reconciliation links. **Never** receive writes from open-source ingest or AI steward jobs. |
| **Staging — candidates** | `Independent Hotel Source Candidates` | Raw, source-specific rows (OSM, Wikidata, brand directory, government registry, Google Places lookup, submitted). One row per source observation; human review before promotion. |
| **Staging — evidence** | `Independent Hotel Source Evidence` | Proof snapshots, match rationale, license checks, steward notes tied to a candidate. |
| **Future master** | `Verified Independent Hotel Census` | Human-approved **golden records** — the intended long-term **clean Dealality hotel master**. Product-facing identity, geography, brand, and permissioned attributes live here after governance review. |

**Data flow (target state):**

```
Open / permissioned sources → Candidates (+ Evidence)
         ↓ human + steward review
Verified Independent Hotel Census (golden master)
         ↓ optional read-only link (not merge)
Hotel Census (legacy reference only)
```

---

## 2. Why Verified should become the clean master

1. **License and provenance** — The legacy census embeds STR/CoStar-derived markets, performance metrics, and proprietary IDs that cannot be replicated from open sources or shown without separate agreements.
2. **Multi-source truth** — A verified row aggregates corroborated facts (OSM geo, Wikidata enrichment, brand directory, government registry, permissioned submission) with explicit source governance fields.
3. **Product safety** — `Can Use In Product`, `Can Show To Users`, and `Can Use For Scoring` are set per record from source policy, not assumed from a bulk import.
4. **Operational clarity** — Staging tables stay messy; verified rows are deduped, named consistently, and tied to approval workflow.
5. **Migration control** — Legacy `Hotel Census` remains until an explicit, approved production migration swaps read paths to the verified master — not a silent schema clone.

Phase 2D DR hotel-focused OSM illustrates the gap: 918 named candidates, ~19% with phone, ~17% with website, 106 likely already in legacy census — none of that should auto-promote without review and verified-field design.

---

## 3. Why we should not clone Hotel Census

The current table has **103 fields**, many tied to STR import batches, rate performance, and contact blocks that are not independently licensable.

| Clone risk | Consequence |
|------------|-------------|
| Copy STR market/submarket semantics | Violates “Dealality-defined geography” rule; conflates restricted vendor taxonomy with product markets |
| Copy ADR/RevPAR/occupancy/rates | Restricted performance data; cannot populate from OSM/Wikidata |
| Copy STR Number / Property ID / Chain ID as “facts” | Implies STR as source of truth for independent census |
| Copy owner/management contact grids | PII and outreach data without permission; must stay internal or submission-sourced |
| Copy `Development Cost` as field meaning | In legacy data, often an import-batch marker, not hotel economics |
| 1:1 field parity expectation | Blocks a smaller, governed schema tuned for Dealality product needs |

**Allowed approach:** Use legacy census only for **reconciliation links** (`Linked Current Census Record`, match confidence/reason) and **gap analysis** — not as a field template.

---

## 4. How fields should be added (phased)

Fields are added to Airtable only after:

1. This schema proposal is reviewed by ops/legal/product.
2. `ensure-independent-census-tables.mjs` (or successor) is updated with an explicit phase flag.
3. `lib/independent-census/fields.js` constants are extended to match Airtable names.
4. Promotion scripts require `--approved-by` and never write to `Hotel Census`.

See **Section E** at the end for Phase A–E implementation order.

---

## 5. Field policy legend

| Column | Meaning |
|--------|---------|
| **Source Type** | Primary origin class: `submitted`, `brand_directory`, `government_registry`, `wikidata`, `osm`, `google_places`, `manual_upload`, `dealality_derived` (Dealality taxonomy/scores), `dealality_ops` (human steward), `reconciliation` (legacy census link metadata only) |
| **Can Use In Product** | `yes` / `no` / `review_required` / `restricted_refresh_required` / `conditional` — mirrors `lib/independent-census/source-policy.js` |
| **Can Show To Users** | Whether end-user UI may display the field value |
| **Can Use For Scoring** | Whether deal readiness / radar / matching scores may consume the field |
| **Internal Only** | If `Yes`, must not appear in user-facing APIs without a separate permission gate |

**Conservative default for unknown or single-source weak fields:** `review_required`, not shown, not scored, manual review required.

---

## 6. Proposed schema by section

### 6.1 Core identity

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Dealality Hotel ID | Single line text (or formula) | Stable platform identifier for the verified record; not STR Property ID | `dh_DO_santo_domingo_001` | Yes | dealality_derived | yes | no | no | Yes | Assigned at promotion; immutable; primary key for internal APIs |
| Verified Hotel Name | Single line text | Human-approved display name | `Embassy Suites by Hilton Santo Domingo` | Yes | dealality_ops | yes | yes | yes | No | Primary user-facing name |
| Normalized Hotel Name | Single line text | Lowercase, diacritic-stripped match key | `embassy suites by hilton santo domingo` | Yes | dealality_derived | yes | no | yes | Yes | Used for dedupe/matching; not shown raw in UI |
| Former Hotel Names | Long text | Prior names / rebrands (comma or JSON list) | `Hotel Hispaniola; Barcelo Santo Domingo` | No | dealality_ops | review_required | conditional | no | No | Show only when sourced from public rebrand evidence or submission |
| Alternative Names | Long text | AKAs, local spellings, OSM alt names | `Embassy Suites SDQ` | No | osm, wikidata, manual_upload | review_required | conditional | no | No | Do not auto-promote unverified OSM name variants |
| Hotel Status | Single select | Lifecycle in Dealality census | `operating` | Yes | dealality_ops | yes | yes | yes | No | Values: `operating`, `pre_opening`, `closed`, `demolished`, `unknown` |
| Open Status | Single select | Operating intent (finer than status) | `open` | No | brand_directory, government_registry, submitted | review_required | yes | no | No | Values: `open`, `temporarily_closed`, `planned`, `unknown` |
| Opening Date | Date | First guest-ready open (verified) | `2015-03-01` | No | brand_directory, government_registry, submitted, wikidata | review_required | yes | no | No | Manual review if sources disagree |
| Last Renovation Date | Date | Last major renovation | `2022-11-15` | No | submitted, brand_directory | review_required | conditional | no | No | Optional product detail |
| Active | Checkbox | Soft delete / include in exports | checked | Yes | dealality_ops | yes | no | yes | No | Uncheck instead of hard delete |

---

### 6.2 Location and geography

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Verified Address | Single line text | Full formatted address (display) | `Av. Sarasota 49, Bella Vista, Santo Domingo` | No | submitted, osm, brand_directory | review_required | yes | no | No | Prefer submission or brand directory over OSM alone |
| Address Line 1 | Single line text | Street number and street | `Av. Sarasota 49` | No | submitted, government_registry | review_required | yes | no | No | |
| Address Line 2 | Single line text | Unit, building, district line | `Bella Vista` | No | submitted | review_required | conditional | no | No | |
| Verified City | Single line text | City or municipality | `Santo Domingo` | Yes | submitted, government_registry, dealality_ops | yes | yes | yes | No | OSM city alone insufficient for `yes` without review |
| Verified State / Province | Single line text | Admin area | `Distrito Nacional` | No | government_registry, submitted | review_required | yes | yes | No | |
| Verified Country | Single line text | ISO-friendly country name | `Dominican Republic` | Yes | submitted, government_registry | yes | yes | yes | No | |
| Verified Postal Code | Single line text | ZIP/postal | `10148` | No | government_registry, submitted | review_required | yes | no | No | |
| Latitude | Number (precision 6) | Verified WGS84 latitude | `18.4567` | No | osm, wikidata, submitted | review_required | conditional | yes | No | **Master coords** from verified source; not Google-sourced unless terms allow permanent storage |
| Longitude | Number (precision 6) | Verified WGS84 longitude | `-69.9421` | No | osm, wikidata, submitted | review_required | conditional | yes | No | Same as latitude |
| Geo Confidence | Single select | Steward confidence in coordinates | `high` | No | dealality_ops | yes | no | yes | Yes | Values: `high`, `medium`, `low`, `unknown` |
| Location Source | Single select | Which source set master coordinates | `submitted` | No | dealality_ops | yes | no | yes | Yes | Values: `submitted`, `osm`, `wikidata`, `government_registry`, `brand_directory`, `manual` |
| Dealality Market | Single line text | **Dealality-defined** commercial market | `Greater Santo Domingo` | No | dealality_derived | yes | yes | yes | No | **Must not** copy STR `Market` or CoStar geography; assign via Dealality taxonomy / steward |
| Dealality Submarket | Single line text | **Dealality-defined** submarket | `Piantini / Bella Vista` | No | dealality_derived | yes | yes | yes | No | **Must not** copy STR `Submarket`; independent assignment |
| Tourism Corridor | Single line text | Leisure/business corridor label | `Punta Cana Resort Corridor` | No | dealality_derived | review_required | conditional | yes | No | Product taxonomy; not STR tract/MSA |
| Demand Node | Single line text | Demand anchor (CBD, airport, convention, beach) | `Las Americas Airport` | No | dealality_derived | review_required | conditional | yes | No | |
| Airport Area Flag | Checkbox | Within airport influence zone | unchecked | No | dealality_derived | yes | conditional | yes | No | |
| CBD Flag | Checkbox | Central business district | unchecked | No | dealality_derived | yes | conditional | yes | No | |
| Beach / Resort Area Flag | Checkbox | Coastal resort zone | checked | No | dealality_derived | yes | conditional | yes | No | |

---

### 6.3 Brand and affiliation

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Current Brand | Single line text | Operating brand label | `Radisson Blu` | No | brand_directory, submitted, wikidata | review_required | yes | yes | No | Not the same as legacy `Affiliation` string; map via Brand Alias separately |
| Brand Family | Single line text | Parent brand family | `Radisson` | No | brand_directory, submitted | review_required | yes | yes | No | |
| Parent Company | Single line text | Corporate parent | `Choice Hotels International` | No | brand_directory, submitted, wikidata | review_required | yes | yes | No | |
| Brand Type | Single select | Franchise vs soft vs independent | `franchise` | No | dealality_ops | review_required | conditional | yes | No | Values: `franchise`, `managed`, `soft`, `independent`, `unknown` |
| Chain Scale / Positioning Tier | Single select | Upscale/midscale/economy/luxury | `upscale` | No | brand_directory, dealality_derived | review_required | yes | yes | No | Dealality-controlled vocabulary; not STR Chain Scale import |
| Soft Brand Flag | Checkbox | Soft collection member | unchecked | No | brand_directory | review_required | yes | yes | No | |
| Conversion Brand Flag | Checkbox | Conversion collection | unchecked | No | brand_directory | review_required | conditional | yes | No | |
| Lifestyle Flag | Checkbox | Lifestyle positioning | unchecked | No | brand_directory | review_required | conditional | yes | No | |
| All-Inclusive Flag | Checkbox | All-inclusive model | unchecked | No | brand_directory, submitted | review_required | yes | yes | No | |
| Resort Flag | Checkbox | Resort positioning | checked | No | osm, brand_directory | review_required | yes | yes | No | Corroborate OSM `tourism=resort` |
| Extended Stay Flag | Checkbox | Extended-stay product | unchecked | No | brand_directory | review_required | conditional | yes | No | |
| Independent Hotel Flag | Checkbox | No major chain affiliation | unchecked | No | dealality_ops | yes | yes | yes | No | |
| Previous Brand | Single line text | Prior brand before conversion | `Sheraton` | No | dealality_ops, manual_upload | review_required | conditional | no | No | |
| Original Affiliation | Single line text | First known affiliation | `Holiday Inn` | No | manual_upload, submitted | review_required | no | no | No | Historical; internal research |
| Year Affiliated | Number | Year joined current brand | `2019` | No | brand_directory, submitted | review_required | conditional | no | No | |
| Brand Verification Source | Single select | Source that confirmed brand | `brand_directory` | No | dealality_ops | yes | no | yes | Yes | Required when Current Brand is set |

---

### 6.4 Physical property attributes

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Room Count | Number | Guest rooms (integer) | `213` | No | government_registry, brand_directory, submitted | review_required | yes | yes | No | Do not copy STR room count without licensed source |
| Floors | Number | Above-grade floors | `12` | No | osm, brand_directory | review_required | conditional | no | No | |
| Total Meeting Space | Number | Meeting space sq ft or sq m (document unit in notes) | `15000` | No | brand_directory, submitted | review_required | conditional | no | No | Store unit convention in ops wiki |
| Largest Meeting Room | Number | Largest room size | `4500` | No | brand_directory | review_required | conditional | no | No | |
| Restaurant Flag | Checkbox | On-site restaurant | checked | No | osm, brand_directory | review_required | conditional | no | No | |
| Bar Flag | Checkbox | Bar/lounge | checked | No | osm | review_required | conditional | no | No | |
| Spa Flag | Checkbox | Spa | unchecked | No | osm, brand_directory | review_required | conditional | no | No | |
| Golf Flag | Checkbox | Golf | unchecked | No | osm, brand_directory | review_required | conditional | no | No | |
| Casino Flag | Checkbox | Casino | unchecked | No | osm, government_registry | review_required | conditional | no | No | |
| Conference / Convention Flag | Checkbox | Major meeting/convention | unchecked | No | brand_directory | review_required | conditional | yes | No | |
| Beachfront Flag | Checkbox | Beachfront location | checked | No | osm, dealality_derived | review_required | conditional | yes | No | |
| Pool Flag | Checkbox | Pool | checked | No | osm | review_required | conditional | no | No | |
| Fitness Center Flag | Checkbox | Fitness center | checked | No | osm, brand_directory | review_required | conditional | no | No | |
| All Suites Flag | Checkbox | All-suites product | unchecked | No | brand_directory | review_required | conditional | yes | No | |
| Indoor Corridors Flag | Checkbox | Interior corridors | checked | No | brand_directory | review_required | no | no | No | |
| Property Type | Single select | Asset class | `full_service` | No | dealality_ops, brand_directory | review_required | yes | yes | No | Values: `full_service`, `select_service`, `extended_stay`, `resort`, `boutique`, `mixed_use`, `other` |
| Service Model | Single select | F&B / service intensity | `full_service` | No | dealality_ops | review_required | yes | yes | No | Values: `full_service`, `limited_service`, `all_inclusive`, `condo_hotel`, `other` |

---

### 6.5 Ownership and operator attributes

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Owner Company | Single line text | Asset owner entity | `ABC Hospitality REIT` | No | submitted, government_registry | conditional | no | yes | Yes | **Not user-facing by default**; show only with permission |
| Owner Type | Single select | Owner category | `institutional` | No | dealality_ops | review_required | no | yes | Yes | Values: `institutional`, `private`, `public`, `family`, `government`, `unknown` |
| Management Company | Single line text | Operator/manager | `XYZ Hotel Management` | No | submitted, brand_directory | conditional | conditional | yes | Yes | Prefer submitted/brand over scraped directories |
| Operator Type | Single select | Operator category | `third_party_manager` | No | dealality_ops | review_required | no | yes | Yes | |
| Management Model | Single select | Franchise/managed/owner-operated | `franchised` | No | dealality_ops, submitted | review_required | conditional | yes | No | |
| Franchise / Managed / Independent Status | Single select | High-level deal structure | `franchise` | No | dealality_ops | review_required | yes | yes | No | |
| Ownership Source | Single select | Source for owner fields | `submitted` | No | dealality_ops | yes | no | yes | Yes | |
| Operator Source | Single select | Source for operator fields | `brand_directory` | No | dealality_ops | yes | no | yes | Yes | |
| Visibility Level | Single select | How much owner/operator detail may surface | `internal_only` | No | dealality_ops | yes | no | yes | Yes | Values: `public_summary`, `authenticated`, `internal_only` |
| Confidentiality Flag | Checkbox | Record has confidential owner/operator data | checked | No | dealality_ops | yes | no | yes | Yes | Gates API field filtering |

**Guardrail:** Owner/operator **contact** fields (name, email, phone, address blocks) are **out of scope** for this verified schema unless added later as a separate **permissioned** table linked 1:1 — not copied from legacy `Hotel Census` owner/management grids.

---

### 6.6 Dealality intelligence fields

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Development Relevance Score | Number (0–100) | Pipeline relevance for development/conversion | `72` | No | dealality_derived | yes | no | yes | Yes | Computed by Dealality models; not sourced from STR |
| Conversion Candidate Flag | Checkbox | Likely conversion opportunity | unchecked | No | dealality_ops | yes | no | yes | Yes | **Manual review required** before setting |
| Rebrand Candidate Flag | Checkbox | Likely rebrand opportunity | unchecked | No | dealality_ops | yes | no | yes | Yes | Manual review |
| Brand White Space Flag | Checkbox | Gap in brand coverage for market | unchecked | No | dealality_derived | yes | no | yes | Yes | |
| Market Priority | Single select | Dealality market tier | `tier_1` | No | dealality_ops | yes | no | yes | Yes | Values: `tier_1`, `tier_2`, `tier_3` |
| Owner Outreach Priority | Single select | Internal outreach queue priority | `high` | No | dealality_ops | yes | no | yes | Yes | **Internal only** |
| Last Reviewed By | Single line text | Steward reviewer | `jane@dealality.com` | No | dealality_ops | yes | no | no | Yes | |
| Last Reviewed Date | Date | Last human review | `2026-05-20` | No | dealality_ops | yes | no | no | Yes | |
| Research Status | Single select | Research queue state | `complete` | No | dealality_ops | yes | no | no | Yes | Values: `not_started`, `in_progress`, `blocked`, `complete` |
| Data Confidence | Single select | Overall field confidence | `medium` | Yes | dealality_ops | yes | conditional | yes | No | Values: `high`, `medium`, `low` |
| Source Confidence | Single select | Confidence in primary source | `medium` | No | dealality_ops | yes | no | yes | Yes | |
| Verification Status | Single select | Promotion lifecycle | `verified` | Yes | dealality_ops | yes | conditional | yes | No | Values: `draft`, `pending_review`, `verified`, `disputed`, `retired` |

---

### 6.7 Source governance fields

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Primary Source Name | Single line text | Label of winning source | `OpenStreetMap` | Yes | dealality_ops | yes | no | yes | Yes | From promotion candidate |
| Primary Source Type | Single select | `osm`, `wikidata`, etc. | `osm` | Yes | dealality_ops | yes | no | yes | Yes | Align with `SOURCE_TYPES` + `google_places`, `submitted` |
| Primary Source URL | URL | Record URL | `https://www.openstreetmap.org/node/123` | No | dealality_ops | yes | no | no | Yes | Attribution where required (ODbL) |
| Primary Source License | Single line text | SPDX or license label | `ODbL` | Yes | dealality_ops | yes | no | no | Yes | |
| Secondary Source Count | Number | Linked evidence / candidate count | `3` | No | dealality_derived | yes | no | no | Yes | Rollup from Evidence table |
| Last Source Refresh Date | Date | Last upstream refresh | `2026-05-01` | No | dealality_ops | yes | no | no | Yes | Required when `Requires Refresh` sources used |
| Last Verified Date | Date | Last human verification pass | `2026-05-20` | No | dealality_ops | yes | conditional | yes | No | |
| Can Use In Product | Checkbox | Record cleared for product APIs | unchecked | Yes | dealality_ops | yes | no | yes | Yes | **Never** auto-set by ingest |
| Can Show To Users | Checkbox | User-facing display allowed | unchecked | Yes | dealality_ops | yes | no | yes | Yes | |
| Can Use For Scoring | Checkbox | Scoring engines may consume | unchecked | Yes | dealality_ops | yes | no | yes | Yes | |
| Requires Attribution | Checkbox | Product must show attribution | checked | No | dealality_ops | yes | no | no | Yes | e.g. OSM ODbL |
| Requires Refresh | Checkbox | Periodic re-fetch required | unchecked | No | dealality_ops | yes | no | no | Yes | e.g. Google Places |
| Requires Manual Review | Checkbox | Block auto-promotion | checked | Yes | dealality_ops | yes | no | no | Yes | Default **checked** |
| Source Risk Level | Single select | Aggregated risk | `medium` | No | dealality_ops | yes | no | yes | Yes | Values: `low`, `medium`, `high` |

---

### 6.8 External reference IDs

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Google Place ID | Single line text | Maps Place ID | `ChIJ...` | No | google_places | restricted_refresh_required | no | no | Yes | Lookup/enrichment only; refresh on schedule; no photos/reviews stored |
| OSM Element ID | Single line text | `node/way/relation` id | `way/44123456` | No | osm | review_required | no | no | Yes | Attribution required if shown |
| Wikidata QID | Single line text | Wikidata entity | `Q12345678` | No | wikidata | yes | conditional | no | No | CC0 entity link |
| Official Registry ID | Single line text | Government license id | `HTL-2019-0042` | No | government_registry | review_required | no | no | Yes | Jurisdiction-specific |
| Brand Directory ID | Single line text | Chain property code | `MAR-12345` | No | brand_directory | review_required | no | no | Yes | Per-chain format |
| STR ID / Industry Reference ID | Single line text | Optional industry crosswalk | `12345` | No | submitted | conditional | no | no | Yes | **Only** owner/brand/operator-submitted or separately licensed — **never** from STR Excel/CoStar export |
| STR ID Source | Single line text | Who provided STR ID | `owner_submitted` | No | submitted | conditional | no | no | Yes | Required if STR ID populated |
| STR ID Source Type | Single select | Permission class | `submitted` | No | submitted | conditional | no | no | Yes | Values: `submitted`, `licensed_partner`, `not_applicable` |
| STR ID Visibility | Single select | Display policy | `internal_only` | No | dealality_ops | yes | no | no | Yes | Default `internal_only` |
| STR ID Use Permission | Single select | Legal use scope | `matching_only` | No | dealality_ops | yes | no | no | Yes | Values: `matching_only`, `display`, `prohibited` |
| STR ID Verified Status | Single select | Steward verification of STR ID | `unverified` | No | dealality_ops | yes | no | no | Yes | Values: `unverified`, `verified`, `rejected` |

---

### 6.9 Reconciliation and approval workflow

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Linked Current Census Record | Link → Hotel Census | Read-only legacy match | (record link) | No | reconciliation | yes | no | no | Yes | **Never** auto-write Hotel Census; link only |
| Current Census Match Confidence | Single select | Match tier vs legacy | `high` | No | reconciliation | yes | no | yes | Yes | `none`, `low`, `medium`, `high` |
| Current Census Match Reason | Long text | Why matched | `name+city+geo 41m` | No | reconciliation | yes | no | no | Yes | From Phase 2C matcher |
| Duplicate Group ID | Single line text | Cluster id for dup review | `dup_DO_2026_05_001` | No | dealality_ops | yes | no | yes | Yes | |
| Merge Status | Single select | Dedupe merge state | `none` | No | dealality_ops | yes | no | no | Yes | Values: `none`, `pending`, `merged`, `rejected` |
| Promotion Status | Single select | Candidate promotion state | `promoted` | Yes | dealality_ops | yes | no | no | Yes | Values: `promoted`, `reverted` |
| Promoted From Candidate | Link → Candidates | Source candidate | (record link) | Yes | dealality_ops | yes | no | no | Yes | Replaces basic `Primary Source Candidate` naming |
| Approved By | Single line text | Human approver | `ops@dealality.com` | Yes | dealality_ops | yes | no | no | Yes | Required on promotion |
| Approved At | Date time | Approval timestamp | `2026-05-20T14:00:00Z` | Yes | dealality_ops | yes | no | no | Yes | |
| Approval Notes | Long text | Reviewer notes | `Matched OSM way; brand confirmed via Choice directory CSV` | No | dealality_ops | yes | no | no | Yes | |

**Existing Phase 2A fields retained:** `Verified Dedupe Key`, `Census Reconciliation Status` (maps to match disposition), `Active` — merge naming with `Promoted From Candidate` / `Linked Current Census Record` during Phase A schema apply.

---

### 6.10 Visibility and permissions

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Record Visibility | Single select | API export scope | `internal` | Yes | dealality_ops | yes | no | yes | Yes | Values: `public`, `authenticated`, `internal`, `restricted` |
| User-Facing Eligible | Checkbox | Passed gates for UI | unchecked | Yes | dealality_ops | yes | no | yes | Yes | Requires `Can Show To Users` + review |
| Internal Notes | Long text | Ops-only notes | `Do not contact owner until Q3` | No | dealality_ops | yes | no | no | Yes | Never expose via public API |
| Confidential Fields Present | Checkbox | Auto or manual flag | checked | No | dealality_derived | yes | no | yes | Yes | Drives field-level redaction |
| Permission Status | Single select | Data permission state | `pending` | No | dealality_ops | yes | no | yes | Yes | Values: `pending`, `granted`, `revoked`, `not_required` |
| Data Use Restrictions | Long text | JSON or text restrictions | `no_google_lat_lng_in_export` | No | dealality_ops | yes | no | yes | Yes | Machine-readable restrictions list (future) |

**Public contact fields (separate from owner/operator):**

| Field Name | Airtable Type | Description | Example Value | Required? | Source Type | Can Use In Product? | Can Show To Users? | Can Use For Scoring? | Internal Only? | Notes / Guardrails |
|------------|---------------|-------------|---------------|-----------|-------------|---------------------|--------------------|-----------------------|----------------|---------------------|
| Verified Website | URL | Public hotel website | `https://www.example.com` | No | submitted, brand_directory, wikidata | review_required | yes | no | No | Already in Phase 2A schema |
| Verified Phone | Phone or single line text | Public reservations desk | `+1-809-555-0100` | No | submitted, brand_directory | review_required | yes | no | No | Not owner direct line |

---

## A. Fields that should not be copied from current Hotel Census

Do **not** add these as sourced or verified fields populated from legacy import or STR/CoStar files:

| Legacy field | Reason |
|--------------|--------|
| STR Number | Proprietary STR identifier; independent census uses optional governed **STR ID / Industry Reference ID** only |
| Property ID | STR property key |
| Chain ID | STR/chain proprietary coding |
| Market | STR-derived market taxonomy — use **Dealality Market** instead |
| Submarket | STR-derived — use **Dealality Submarket** instead |
| MSA | STR/Census proprietary geography |
| Tract | STR geography |
| ADR | Restricted performance |
| RevPAR | Restricted performance |
| Occupancy Rate | Restricted performance |
| Single High/Low Rate, Double High/Low Rate, Suite High/Low Rate | Restricted rate bands |
| Development Cost | Import-batch marker / non-public economics in legacy usage |
| Owner contact block (name, email, phone, address, website) | Not permissioned from open sources |
| Management contact block | Same |
| Star Rating (STR-sourced) | Do not import; use verified amenity/positioning flags or submission |
| Region / Sub-Continent / Continent (if STR-harmonized) | Rebuild under Dealality geography if needed |
| Include in Brand Explorer | Product flag on legacy table — reintroduce on verified master only after migration plan |
| Formula fields tied to STR affiliation dates | Recompute from verified dates if needed |

---

## B. Fields that can be independently recreated

These concepts are safe to populate from open, official, or permissioned sources (with review):

| Concept | Typical sources |
|---------|-----------------|
| Hotel name | OSM, Wikidata, brand directory, submitted |
| Address, city, country | Government registry, submitted, brand directory |
| Latitude / longitude | OSM, Wikidata, submitted (not Google as permanent master without terms review) |
| Public website / phone | Brand directory, Wikidata, submitted |
| Brand / parent company | Brand directory, submitted, Wikidata |
| Room count | Government registry, brand directory, submitted |
| Property type / service model | Brand directory, steward taxonomy |
| Amenity flags | OSM, brand directory |
| Opening date | Brand directory, government registry, submitted |
| Dealality Market / Submarket | **Dealality-derived only** — steward assignment |

---

## C. Fields requiring manual review

Always human-reviewed before `Verification Status = verified` or product flags set:

- Owner company, owner type, management company, operator type
- Current brand, brand family, parent company, chain scale
- Room count (when single weak source)
- Open status / opening date (conflicting sources)
- Conversion candidate flag, rebrand candidate flag
- Dealality Market, Dealality Submarket, tourism corridor, demand node
- Any Google-derived field
- Brand-directory-derived brand/URL (per-source terms)
- Government registry legal name vs marketing name
- Linked Current Census Record and match confidence (steward confirms)
- STR ID / Industry Reference ID (permission check)
- Can Use In Product / Can Show To Users / Can Use For Scoring checkboxes

---

## D. Fields that should remain internal-only

Default to internal-only (API redaction unless explicit permission):

- Dealality Hotel ID (display may use slug later)
- Normalized Hotel Name
- Owner company, owner type, ownership source
- Management company (detail), operator type (detail)
- Development relevance score, conversion/rebrand flags, white space flag
- Market priority, owner outreach priority
- All source governance checkboxes and risk notes
- Reconciliation links and match reason (legacy census record id)
- External IDs except Wikidata QID / public website when approved
- STR ID and all STR ID governance subfields unless `STR ID Visibility = display` with legal approval
- Internal Notes, Data Use Restrictions
- Geo confidence, location source (optional summary in UI)

---

## E. Recommended phased Airtable implementation

| Phase | Scope | Approx. fields |
|-------|--------|----------------|
| **Phase A** | Core identity, location (incl. Dealality Market/Submarket placeholders), source governance, promotion/reconciliation links, Active | ~35 |
| **Phase B** | Brand/affiliation, physical attributes, public website/phone | ~30 |
| **Phase C** | Owner/operator (internal), Dealality intelligence | ~20 |
| **Phase D** | External reference IDs (incl. governed STR ID block), visibility/permissions | ~25 |
| **Phase E** | Production migration planning: API read model, Brand Explorer cutover checklist, legacy link deprecation policy | Process doc |

**Prerequisite for each phase:** Update `docs/independent-hotel-census-pipeline.md` phase table and `lib/independent-census/fields.js` constants; run `independent-census:schema:check` before `schema:apply`.

---

## F. Dependencies and risks

| Dependency | Impact |
|------------|--------|
| Candidate + Evidence ingest (Phase 3) | Verified rows need linked candidates and evidence before promotion |
| Source policy module | Record-level flags must derive from `getSourcePolicy()` at promotion time |
| Brand Alias Mapping | `Current Brand` must map to product rollups separately — not legacy `Affiliation` copy |
| Dealality Market taxonomy | Requires steward playbook before populating market/submarket at scale |
| Google Places terms | Blocks permanent master lat/lng and display fields until legal review |
| STR ID governance | Optional field; misuse risk if populated from legacy census import |
| Hotel Census link fields | Manual Airtable link fields on staging/verified — API cannot add inverse on legacy table |

| Risk | Mitigation |
|------|------------|
| Accidental legacy census write | Pipeline flags + no `--apply` on matchers/importers |
| Schema sprawl | Phased A–E; defer owner contacts to separate permissioned table |
| OSM-over-breadth promotion | Hotel-focused filters + manual review defaults |
| Product cutover too early | `Can Use In Product` default false until migration approval |

---

## G. Mapping from current minimal verified schema (Phase 2A)

| Existing field (2A) | Proposed evolution |
|---------------------|-------------------|
| Verified Hotel Name | Retain |
| Verified Address / City / State / Country / Postal | Retain; split Address Line 1/2 in Phase A |
| Verified Latitude / Longitude | Retain; add Geo Confidence, Location Source |
| Verified Website / Phone | Retain |
| Verified Brand Label | Rename conceptually to **Current Brand**; keep label during transition |
| Primary Source Candidate | Rename to **Promoted From Candidate** |
| Approved At / By / Notes | Retain |
| Census Reconciliation Status | Retain; align with **Current Census Match Confidence** |
| Linked Census Record | Rename to **Linked Current Census Record** |
| Verified Dedupe Key | Retain |
| Active | Retain |

---

## H. Phase 2H completion checklist

- [x] Schema proposal documented
- [ ] Airtable fields created (explicitly out of scope)
- [ ] `fields.js` extended (future phase)
- [ ] Promotion script (Phase 4)

**This document alone does not change code, Airtable, or production systems.**
