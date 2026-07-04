# Operator Alignment — Phase 5B Schema & Mapping Implementation

**Date:** 2026-05-25  
**Phases:** 5B-1 (Airtable schema) + 5B-2 (mappings, Deal Intake, Operator Setup UI)  
**Audit reference:** [operator-alignment-scoring-data-quality-audit.md](./operator-alignment-scoring-data-quality-audit.md)

---

## Summary

- **40 new Airtable fields** created (0 skipped, 0 failed).
- **7 existing fields reused** (not duplicated).
- **Scoring weights unchanged**; `scoreOperatorMatchForDeal` only extended `firstPresent` / `collectPresentList` keys so new prefill data is visible to future scoring work.
- **No changes** to Brand Alignment Snapshot, Operator Capability Snapshot, or OAS PDF layout.

---

## Airtable backup

| Artifact | Path |
|----------|------|
| Pre-change inventory | `reports/operator-alignment-5b-schema-backup-2026-05-25.json` |
| Apply log | `reports/operator-alignment-5b-schema-apply-2026-05-25.json` |

Script: `node scripts/operator-alignment-phase-5b-airtable-schema.mjs --export-only` / `--apply`

---

## Fields added by table

### Operator Setup - Platform & Markets

| Field | Type |
|-------|------|
| Active Countries | multipleSelects |
| Active Markets / Cities | multipleSelects |
| Market Presence Type | multipleSelects |

### Operator Setup - Profile & Positioning

| Field | Type |
|-------|------|
| Service Models Supported | multipleSelects |
| Brand Families Operated | multipleSelects |
| Soft Brand / Lifestyle Experience | singleSelect |

### Operator Setup - Commercial Fit & Terms

| Field | Type |
|-------|------|
| Management Structures Supported | multipleSelects |
| New-Build Opening Experience | singleSelect |
| Pre-Opening Support Capability | singleSelect |
| Conversion / Reflag Experience | singleSelect |
| Minimum Key Count | number |
| Similar Project Case Studies | multilineText |

### Operator Setup - Governance, Delivery & Diligence

| Field | Type |
|-------|------|
| Offered Services | multipleSelects |
| Owner Reporting Level | singleSelect |
| F&B Capability Level | singleSelect |
| Revenue Management Capability | singleSelect |
| Sales Platform | multipleSelects |
| Governance Cadence | singleSelect |

### Operator Setup - Master (admin)

| Field | Type |
|-------|------|
| Data Confidence Level | singleSelect |
| Source Type | multipleSelects |
| Last Updated Date | date |

### Strategic Intent - Operational - Key Challenges

| Field | Type |
|-------|------|
| Operator Review Status | singleSelect |
| Preferred Management Structure | multipleSelects |
| Required Operator Services | multipleSelects |
| Must-Have Operator Services | multipleSelects |
| Nice-to-Have Operator Services | multipleSelects |
| Market Presence Requirement | singleSelect |
| Pre-Opening Support Needed | singleSelect |
| Owner Reporting Expectations | singleSelect |
| Brand / Operator Responsibility Split | singleSelect |
| Owner Control Preference | singleSelect |
| Commercial Priority | multipleSelects |
| Local Labor / HR Support Needed | singleSelect |
| Procurement Support Needed | singleSelect |
| Owner Internal Ops Capability | singleSelect |
| Brand Agreement Structure | singleSelect |
| Operating Model | singleSelect |
| Operator Scope | multipleSelects |

### Deals

| Field | Type |
|-------|------|
| F&B Complexity | singleSelect |
| Opening Timeline | singleSelect |

---

## Fields reused (not created)

| Field | Table | Notes |
|-------|-------|-------|
| chainScalesSupported | Profile | Canonical chain scales; UI options aligned via fixture |
| Services Required From Operator | SI | Legacy list kept; parallel **Required Operator Services** added |
| Operator Strategy Status | SI | Kept; parallel **Operator Review Status** added |
| Must-Haves From Brand/Operator | SI | Kept; parallel **Must-Have Operator Services** added |
| Preferred Deal Structure | MP | **Not renamed** — label clarified as brand/franchise economics |
| bf_selected_deal_structures | Commercial | Legacy; **Management Structures Supported** is OAS-canonical |
| specificMarkets | Platform | Long text kept; **Active Markets / Cities** is structured parallel |
| Case Studies child table | — | **Similar Project Case Studies** text is optional summary only |

---

## Deal structure normalization (sample deal)

| Legacy field | Example value | New clarifying fields |
|--------------|---------------|------------------------|
| Preferred Deal Structure (MP) | Franchise Only | **Brand Agreement Structure** = Franchise |
| — | — | **Operating Model** = Third-party managed |
| — | — | **Preferred Management Structure** = Full third-party management |

This documents the Aeropuerto Cancún conflict without overwriting legacy values.

---

## Code & mapping files updated

| File | Change |
|------|--------|
| `lib/operator-alignment-field-options.js` | Standard option lists + Airtable name constants |
| `lib/operator-alignment-prefill-map.js` | Airtable title → camelCase prefill |
| `api/lib/operator-setup-new-base-read.js` | Calls prefill alias mapper |
| `api/lib/operator-setup-new-base-writer.js` | Master admin fields write |
| `api/lib/third-party-operator-new-two-field-bindings.json` | +22 bindings |
| `api/lib/operator-setup-new-base-build-sheet-rows.json` | Regenerated (103 rows) |
| `api/schemas/deal-setup-fields.js` | SI + Deals form field lists, multi-select keys |
| `api/my-deals.js` | `firstPresent` includes new prefill keys (read compatibility only) |
| `public/fixtures/operator-alignment-field-options.json` | Client option source |
| `public/js/oas-inject-form-fields.js` | Injects Deal Intake + Operator Setup fields |
| `public/new-deal-setup.html` | Inject containers + script |
| `public/third-party-operator-setup-new-two.html` | Inject container + script |

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/operator-alignment-phase-5b-airtable-schema.mjs` | Backup + create fields |
| `scripts/append-operator-alignment-5b-bindings.mjs` | Append writer bindings |
| `scripts/validate-operator-alignment-phase-5b.mjs` | Phase 5B validation |
| `scripts/generate-operator-setup-build-sheet-rows.mjs` | Regenerate build sheet |

---

## Backfill templates

| File | Purpose |
|------|---------|
| `docs/operator-alignment-sample-backfill-template.csv` | 10 active CALA sample operators |
| `docs/deal-intake-operator-fields-backfill-template.csv` | Deal-side fields incl. `recIeGRZP21udmTnt` example |

**No automatic operator backfill** was run.

### Deal-side backfill (2026-05-25)

- Script: `scripts/backfill-deal-operator-alignment-fields.mjs` (dry-run default, `--apply` to write)
- Applied to **12 CALA sample deals** via `--sample-deals --apply`
- Report: `reports/deal-operator-alignment-backfill-2026-05-25T174024.json`
- Population log: `docs/deal-intake-operator-fields-population-log.md`
- Score comparison: `docs/operator-alignment-phase-5b-after-backfill-score-comparison.md`
- Legacy **Preferred Deal Structure** (MP) not overwritten; new SI fields clarify franchise vs third-party path
- **Phase 5E (2026-05-25):** Scoring now reads structured fields — `docs/operator-alignment-phase-5e-score-wiring-results.md`

---

## Validation

```bash
node scripts/validate-operator-alignment-phase-5b.mjs
```

Checks: Airtable columns exist, deal-setup field lists, build-sheet form names, weights still 18 for geography, OCS/BAS not coupled to operator match weights.

---

## Known risks

| Risk | Mitigation |
|------|------------|
| Duplicate service fields on SI | Dual-read period; migrate deals to structured multis over time |
| Operating Model name vs Current Operating Model (Deals) | SI **Operating Model** is target path; Deals field unchanged |
| Master fields not in build sheet | Written via `createOrUpdateOperatorMaster` body map |
| Option drift HTML vs Airtable | Single source: `lib/operator-alignment-field-options.js` → public fixture JSON |
| Scoring still uses legacy logic | Phase **5E** — structure token map, gap handling |

---

## Remaining work

| Phase | Scope |
|-------|--------|
| **5C** | Operator Setup UX polish, required-on-publish rules, backfill active operators |
| **5D** | Deal Intake validation + readiness for new SI fields |
| **5E** | Scoring logic + missing-data → Insufficient Data / Needs Validation |
| **5F** | Narrative differentiation (remove hardcoded OAS card bullets) |
| **5G** | Operator Explorer deal-context badges |

---

## Confirmations

- [x] No `OPERATOR_MATCH_WEIGHTS` changes
- [x] No Brand Alignment Snapshot edits
- [x] No Operator Capability Snapshot edits
- [x] No OAS PDF layout edits
- [x] No Airtable field renames or deletes
