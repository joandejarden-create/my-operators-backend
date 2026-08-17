# Deals Schema Finalization Plan

**Date:** 2026-07-06  
**Base audited:** `appvtnDurnMSjINP6` (Deal Capture MVP)  
**Authority:** [airtable-deals-fields.md](./airtable-deals-fields.md), [reports/airtable-deals-schema-diff.md](../../reports/airtable-deals-schema-diff.md), `api/schemas/deal-setup-fields.js`

> **Planning doc.** Schema setup: `npm run setup-deals-schema-phase5b-p1 -- --dry-run` then `--apply`. Re-audit: `npm run audit-airtable-deals-schema`.

---

## Implementation Status (2026-07-06)

**Founder decisions implemented** via `scripts/setup-deals-schema-phase5b-p1.mjs`:

| Field | Table | Airtable action | Code/docs |
|-------|-------|-----------------|-----------|
| `Preferred Operator Management Structure` | Market - Performance | Create via setup script | `OAS_DEAL_MP_FIELD_NAMES` — **Deal Setup UI** (Deal & Capital Structure) |
| `Operator Structure Intent` | Strategic Intent | Create via setup script | `OAS_DEAL_SI_FIELD_NAMES` — **Deal Setup UI** (Third-Party Operator / OAS inject) |
| `Ownership Type Other Text` | Location | Create (UI has Other) | Form map unchanged |
| `Zoning Status Other Text` | Location | Create (UI has Other) | Form map unchanged |
| `Broker/Advisor Company and Contract Details` | Contact & Uploads | **Not created** | Maps to `Working with Broker/Advisor? Text` |
| `Brand Internal Notes` | BDR | **Not created** | Deprecated; use Internal follow-up notes |
| `F&B Complexity Level` | SI | **Not created** | Deals `F&B Complexity` is OAS SSOT |
| `Proposal Submissions` table | — | **Not created** | Remains optional in audit |
| Governance/validation columns | — | **Not in scope** | Separate project |

**Post-apply audit (2026-07-06):** 348 exact matches, 6 alias matches, **0 missing**, Phase 5B **14/15** confirmed. See `reports/deals-schema-phase5b-p1-setup.md`.

---

## Purpose

Close the remaining Deals workflow schema gaps **before** future feature work (Operator Alignment scoring credibility, Deal Setup write completeness, BDR notes, governance/validation fields) depends on columns that do not exist or are misnamed in the live base.

The alias-aware audit (`npm run audit-airtable-deals-schema`) reduced false positives. What remains is a small set of **true gaps** plus **Phase 5B proposed fields** that need a product decision before Airtable or code changes.

---

## Current Audit Status

| Metric | Value |
|--------|-------|
| Tables found | **11 / 12** |
| Proposal Submissions | **Optional — not found** in audited base |
| Exact field matches | **341** |
| Alias field matches | **7** |
| Unresolved missing fields | **0** (after 2026-07-06 setup apply) |
| Phase 5B fields confirmed live | **14 / 15** (`F&B Complexity Level` intentionally skipped) |
| Live fields not in registry | **155** (expected: formulas, rollups, legacy `DELETE>>>>`, UI-only, reverse links) |

**Alias matches already resolved (no Airtable change):**

- `Site/Development Restrictions Description` → `Site Restrictions Describe` (Location)
- Strategic Intent `* Other` → `* Other Text` columns
- `Contact Source` → `Contact Info Type`
- `Secondary Contact` → `Secondary Contact (Name & Email)`
- `Next Follow-up Notes (External)` → `Next Follow-up Notes` (BDR)

**Tables confirmed live with new field-level docs:** Deal Activity Log (10 columns), Deal Room Documents (8 columns).

---

## Remaining Missing Fields

### 1. `Ownership Type Other Text`

| | |
|--|--|
| **Table** | Location & Property |
| **Classification** | Confirmed Missing |
| **Source / code** | `LOCATION_FORM_TO_AIRTABLE` in `api/schemas/deal-setup-fields.js`; form inputs in `public/deal-setup.html`, `public/new-deal-setup.html` (`ownershipTypeOtherBlock`) |
| **Product importance** | **Medium** — only when owner selects "Other" for Ownership Type |
| **Recommended action** | **Option A (preferred if Other is used):** Add long-text column `Ownership Type Other Text` on Location & Property with same naming as form map. **Option B:** Remove/hide Other path in UI until column exists. **Option C:** Map to an existing free-text field only if product accepts semantic loss (none identified today). |
| **Risk if ignored** | Owner "Other" ownership detail is **silently dropped** on PATCH; readiness may flag incomplete Location data without surfacing why. |
| **Airtable change needed?** | **Yes** — if Option A. No — if Option B (UI/docs only). |

### 2. `Zoning Status Other Text`

| | |
|--|--|
| **Table** | Location & Property |
| **Classification** | Confirmed Missing |
| **Source / code** | `LOCATION_FORM_TO_AIRTABLE`, `ownershipTypeOtherText` / `zoningStatusOtherText` keys in `deal-setup-fields.js` |
| **Product importance** | **Medium** — only when Zoning Status = Other |
| **Recommended action** | Same pattern as Ownership Type Other Text: add column **or** disable Other path until approved. |
| **Risk if ignored** | Zoning "Other" free text not persisted; compliance/site-control narrative incomplete. |
| **Airtable change needed?** | **Yes** — if persisting Other path. |

### 3. `Broker/Advisor Company and Contract Details`

| | |
|--|--|
| **Table** | Contact & Uploads |
| **Classification** | Needs Manual Verification |
| **Source / code** | `CONTACT_UPLOADS_FORM_FIELDS` in `deal-setup-fields.js` (no entry in `CU_FORM_TO_AIRTABLE` — form name = Airtable name) |
| **Live substitutes** | `Working with Broker/Advisor?` (select), `Working with Broker/Advisor? Text`, `Broker/Firm Name` |
| **Product importance** | **Low–Medium** — advisor workflow context, not scoring-critical |
| **Recommended action** | **Documentation/code alignment (no new column):** Map form field `Broker/Advisor Company and Contract Details` to `Working with Broker/Advisor? Text` and/or split writes across `Broker/Firm Name` + text field. Update `CU_FORM_TO_AIRTABLE` after Joan confirms intended UX. |
| **Risk if ignored** | Broker contract narrative may not save; owners may think data was stored. |
| **Airtable change needed?** | **No** — if mapping to existing live columns. **Yes** — only if product requires a single combined long-text field distinct from live split. |

### 4. `Brand Internal Notes`

| | |
|--|--|
| **Table** | Brand Deal Requests |
| **Classification** | Confirmed Missing |
| **Source / code** | `AT_BRAND_INTERNAL_NOTES_LEGACY` in `api/brand-deal-requests.js` — read fallback only; primary column is `Next Follow-up Notes (Internal)` |
| **Product importance** | **Low** — legacy; internal notes path already migrated in code |
| **Recommended action** | **Documentation/code alignment only:** Remove from schema audit "missing" priority; confirm `Next Follow-up Notes (Internal)` is the sole write target. Add Airtable column only if brands need a **separate** internal field from follow-up notes. |
| **Risk if ignored** | None for current code — fallback read returns empty. Historical data in legacy column name would be invisible if any old base had it. |
| **Airtable change needed?** | **Optional cleanup** — add only if product wants two distinct internal note fields. |

---

## Phase 5B Gaps

Four fields from [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md) are **not live** in the audited base. Twelve others are confirmed (including alias matches for slash variants).

### 1. `Preferred Operator Management Structure`

| | |
|--|--|
| **Table** | Market - Performance - Deal & Capital Structure |
| **Priority** | P1 |
| **Intended purpose** | Owner-facing **operator** management path (third-party mgmt, franchise+operator, owner-operated, etc.) — **split from** brand `Preferred Deal Structure` |
| **OAS scoring depends on it?** | **Planned yes** — today scoring uses SI `Preferred Management Structure` + legacy MP `Preferred Deal Structure` via `scoreDealStructureFactor` (`lib/operator-alignment-scoring-factors.js`). Without this column, franchise-only deals conflate brand economics with operator structure (documented suppressor in Phase 5B audit). |
| **Live substitute?** | **Partial:** `Preferred Deal Structure` (MP) + `Preferred Management Structure` (SI) + `Brand Agreement Structure` (SI). Tradeoff: brand franchise path still confounded with operator management intent. |
| **Recommended action** | **Founder approval → manual Airtable create** (multiple select, options per alignment doc). Then add to `OAS_DEAL_*` maps and Deal Setup intake. Do not write until column exists. |
| **Priority** | **P1 — required before credible OAS structure scoring** |

### 2. `Operator Structure Intent`

| | |
|--|--|
| **Table** | Strategic Intent - Operational - Key Challenges |
| **Priority** | P1 |
| **Intended purpose** | Clarifies owner path when MP shows Franchise Only or ambiguous structure |
| **OAS scoring depends on it?** | **Planned yes** — not in `OAS_DEAL_SI_FIELD_NAMES` today; scoring uses structured SI fields when present |
| **Live substitute?** | **Partial:** `Operating Model`, `Operator Scope`, `Plan to Self-Manage or Hire Third Party?` |
| **Recommended action** | Create after P1 MP field decision; wire into `operator-alignment-deal-normalize.js` and scoring factor notes |
| **Priority** | **P1 — required before next OAS scoring iteration** |

### 3. `Brand Affiliation Path`

| | |
|--|--|
| **Table** | Strategic Intent |
| **Priority** | P2 |
| **Intended purpose** | Pathway narrative (Unbranded / Soft / Hard / Franchise / Brand-Managed) |
| **OAS scoring depends on it?** | **Partial / future** — not in current `OAS_DEAL_SI_FIELD_NAMES`; audit doc marks scoring "Partial" |
| **Live substitute?** | **Partial:** Deals `Current Brand Affiliation`, SI brand-path fields (`Soft vs Hard Brand Preference`, etc.) |
| **Recommended action** | **Optional for MVP OAS** — add when pathway table UX is built; improves narrative differentiation |
| **Priority** | **P2 — nice-to-have before Explorer pathway work** |

### 4. `F&B Complexity Level`

| | |
|--|--|
| **Table** | Strategic Intent (proposed); related live field on **Deals** |
| **Priority** | P2 |
| **Intended purpose** | Normalized F&B complexity band for OAS narrative |
| **OAS scoring depends on it?** | **No today** — `OAS_DEAL_DEALS_FIELD_NAMES.fbComplexity` maps to live **`F&B Complexity`** on Deals (`operator-alignment-deal-normalize.js`) |
| **Live substitute?** | **Yes:** `F&B Complexity` on Deals + amenities (`F&B Outlets?`, program fields on live Deals). Tradeoff: SI-level normalized band vs owner-entered Deals field; scoring already uses Deals column. |
| **Recommended action** | **Defer Airtable create** — document that `F&B Complexity` (Deals) is the live SSOT unless product explicitly wants SI duplicate. Remove from "blocking" gap list. |
| **Priority** | **P3 / optional cleanup** — documentation alignment only unless SI column is product-required |

---

## Lease Structure Link Issue

### Current code expectation

- `LEASE_STRUCTURE_LINK_FIELD` = `"Lease Structure"` on **Deals** (`api/schemas/deal-setup-fields.js`, env `AIRTABLE_DEALS_LINK_FIELD_LEASE_STRUCTURE`)
- Child table link: `LS_DEAL_LINK_FIELD` = `"Deal_ID"` on **Lease Structure**
- Read: `getLinkedLeaseStructureId()` then fallback `findLeaseStructureRecordIdByDealId()` (`api/my-deals.js`)
- Write: PATCH/create child row with `[Deal_ID]: [dealRecordId]`; does **not** require Deals reverse link

### Live schema reality

- **Lease Structure** child table exists with `Deal_ID` link to Deals
- **No** `Lease Structure` link field on Deals row in audited base
- Read/write paths **already work** via `Deal_ID` fallback (confirmed in code comments at create path)

### Risk

| Risk | Severity |
|------|----------|
| Extra API scan on every lease load (`findLeaseStructureRecordIdByDealId`) | Low performance |
| Registry/docs imply Deals link exists — confuses future builds | Medium documentation |
| If another base **does** have Deals→Lease link, env override still valid | Low |

### Recommended action

| Action | Type |
|--------|------|
| Document live pattern: child `Deal_ID` is authoritative in MVP base | Documentation/code alignment only |
| Optional: add `Lease Structure` link on Deals for faster reads (bidirectional) | Optional Airtable cleanup |
| Update `airtable-deals-fields.md` + registry (already notes mismatch) | Done / maintain |
| Do **not** block feature work on adding Deals link — code handles absence | — |

---

## Recommended Airtable Changes

Manual or controlled `ensure-*` script **after** founder approval. **Do not run from audit script.**

| Change | Classification |
|--------|----------------|
| Add `Preferred Operator Management Structure` (MP, multiple select) | **Required before next OAS/scoring work** |
| Add `Operator Structure Intent` (SI, single select) | **Required before next OAS/scoring work** |
| Add `Ownership Type Other Text` (Location, long text) | **Required before next deal setup/write-path work** — if Other path stays in UI |
| Add `Zoning Status Other Text` (Location, long text) | **Required before next deal setup/write-path work** — if Other path stays in UI |
| Add `Brand Affiliation Path` (SI, single select) | **Optional cleanup** — pathway narrative P2 |
| Add `F&B Complexity Level` (SI) | **Optional cleanup** — likely **skip**; use Deals `F&B Complexity` |
| Add `Brand Internal Notes` (BDR) | **Optional cleanup** — only if distinct from Internal follow-up notes |
| Add `Broker/Advisor Company and Contract Details` (CU) | **Skip** — prefer code map to live split columns |
| Add `Lease Structure` link on Deals | **Optional cleanup** — performance/convenience only |
| Create `Proposal Submissions` table | **Optional cleanup** — only if proposal history snapshots still planned |
| Create governance columns (`Validation Status`, `Company Validated`, etc.) | **Separate track** — see [INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md); not part of this audit closure |

---

## Recommended Code/Docs Changes

| Item | Type | Notes |
|------|------|-------|
| Map `Broker/Advisor Company and Contract Details` in `CU_FORM_TO_AIRTABLE` | Code (after verification) | Target live `Working with Broker/Advisor? Text` and/or `Broker/Firm Name` |
| Reclassify `Brand Internal Notes` in audit registry | Docs/registry | `Deprecated / legacy read-only` |
| Map `Site/Development Restrictions Description` → `Site Restrictions Describe` in `LOCATION_FORM_TO_AIRTABLE` | Code alignment | Alias confirmed live |
| Document `F&B Complexity` (Deals) as OAS F&B input SSOT | Docs | Defer `F&B Complexity Level` SI column |
| Add Phase 5B fields to `OAS_DEAL_*` maps **after** Airtable create | Code | With `ensure-operator-alignment-*` options |
| Deal Activity Log / Deal Room Documents write maps | Code (later) | Field lists in registry; no write path yet |
| `npm run audit-airtable-deals-schema` after any schema change | Process | Re-verify diff report |

---

## Decision Needed From Founder

1. **Phase 5B P1 approval** — Create `Preferred Operator Management Structure` and `Operator Structure Intent` in Airtable now, or defer OAS scoring improvements?
2. **Location Other paths** — Keep Ownership Type / Zoning Status "Other" in Deal Setup UI? If yes, approve two new long-text columns.
3. **Broker/advisor fields** — Is the live split (`Working with Broker/Advisor? Text` + `Broker/Firm Name`) sufficient, or is a single combined contract-details field required?
4. **Brand Internal Notes** — Confirm `Next Follow-up Notes (Internal)` is the only internal BDR notes column going forward.
5. **F&B Complexity Level** — Confirm Deals `F&B Complexity` is sufficient (recommended) vs creating SI duplicate.
6. **Proposal Submissions** — Is proposal history snapshot table still planned for this base?
7. **Brand/Operator validation fields** — Approve governance column design before any Airtable create (separate from this plan's core gaps).

---

## Recommended Sequence

```text
1. Founder decisions (Phase 5B P1, Location Other, broker mapping, Proposal Submissions)
2. Documentation/code alignment (no Airtable)
   - Broker field map
   - Site Restrictions Describe alias in LOCATION_FORM_TO_AIRTABLE
   - Brand Internal Notes registry deprecation
   - F&B Complexity SSOT note
3. Airtable manual creates (controlled, one table at a time)
   - P1: Preferred Operator Management Structure, Operator Structure Intent
   - Write-path: Location Other Text columns (if UI kept)
4. npm run audit-airtable-deals-schema → confirm 0 blocking missing
5. Code: OAS maps + Deal Setup exposure for new P1 columns
6. Optional: Brand Affiliation Path, Deals Lease link, Proposal Submissions table
7. Governance / validation columns — separate project after intelligence governance sign-off
```

---

## Do Not Do Yet

- **Do not** build Brand/Operator **validation** or **Company Validated** write paths until governance field design is approved and columns exist ([INTELLIGENCE_GOVERNANCE.md](../data-intelligence/INTELLIGENCE_GOVERNANCE.md)).
- **Do not** increase OAS scoring weight on structure factors until `Preferred Operator Management Structure` exists or explicit legacy fallback is documented in product copy.
- **Do not** add Deal Setup required validation for Location Other text until columns exist.
- **Do not** create `F&B Complexity Level` on SI while Deals `F&B Complexity` is live SSOT — avoids duplicate owner input.
- **Do not** auto-create Airtable fields from scripts without `--dry-run` review and founder approval for Phase 5B.
- **Do not** treat 155 undocumented live fields as gaps — add to registry only when code reads/writes them.
- **Do not** block lease read/write on missing Deals→Lease link — child `Deal_ID` pattern is live and supported.

---

## Related

- [airtable-deals-fields.md](./airtable-deals-fields.md)
- [DATA_DICTIONARY.md](./DATA_DICTIONARY.md)
- [reports/airtable-deals-schema-diff.md](../../reports/airtable-deals-schema-diff.md)
- [operator-alignment-recommended-airtable-fields.md](../operator-alignment-recommended-airtable-fields.md)
- [BUILD_DECISIONS.md](../ai-build-system/BUILD_DECISIONS.md)
