# Approved Intelligence → Platform Field Publishing v1

**Date:** 2026-07-06  
**Status:** Design + read-only audit (no product-field writes)  
**Command:** `npm run approved-intelligence-field-publishing-audit`

> **Authority:** [dealality-intelligence-production-workflow-v1.md](./dealality-intelligence-production-workflow-v1.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [brand-operator-validation-fields-plan.md](./brand-operator-validation-fields-plan.md)

---

## 1. Purpose

This document defines the **bridge** from Dealality’s **governed evidence layer** (Partner Intelligence) to **product-facing platform data** used by Explorer profiles, alignment snapshots, Deal Readiness, outreach prep, and future matching logic.

**Core principle:** Approved facts are **evidence**, not automatic truth. They support trust labels, summaries, and steward-reviewed field updates — they must not blindly overwrite human-curated product fields, governance attestations, or scoring inputs.

**v1 scope:** Design, destination-field audit, read-only classification report. **No Airtable writes** to Setup tables, presentation rows, or scoring fields.

---

## 2. Data layers

### Layer 1 — Evidence Layer

| Artifact | Role |
|----------|------|
| **Source Library** | Registered URLs/files with stewardship status |
| **Extracted Facts** | Candidate values + evidence text + review status |
| **Source ↔ fact links** | Provenance chain for every claim |
| **Human Review Status** | Pending / Approved / Edited / Rejected |

Facts stay in this layer until explicitly approved and classified for publish.

### Layer 2 — Governance Layer

Profile-level trust metadata on **Brand Setup - Brand Basics** or **Operator Setup - Master**:

- Validation Status  
- Usage Permission  
- Source Type / Source Region  
- Last Reviewed Date  
- Confidence Level  
- External Display Status  
- Evidence Notes  
- **Company Validated** / **Company Validation Date** (attestation only — never from PI extraction)

Governance is published via `publish-partner-intelligence-profile-governance` — separate from product field publishing.

### Layer 3 — Product Data Layer

Platform-facing content consumed by:

| Consumer | Data source today |
|----------|-------------------|
| **Brand Explorer** | Brand Basics + Explorer Presentation slots + brand-library API |
| **Operator Explorer** | Operator Setup child tables → prefill / explorer JSON |
| **BAS / OAS / OCS** | Read Setup + deal context; scoring rules separate |
| **Deal Readiness** | Deal + profile reads; gap questions |
| **Outreach prep** | Profile summaries + Helena intake |
| **Future matching** | Explicit rules only — not auto-inferred from weak facts |

Optional intermediate overlay: **Partner Intelligence - Published Explorer Fields** (read merge via `publish-overlay.js` when enabled).

---

## 3. Mapping strategy

Stable fact keys (`op.*`, `be.*`) map through `partner-intelligence-explorer-field-registry.js` to:

1. **Registry metadata** — Explorer tab/section, `responsePath`, `prefillKey` / `slotKey`  
2. **Destination table + column** — from Operator Setup build sheet or Brand Basics / Presentation  
3. **Publish mode** — Evidence Only / Suggested / Controlled / Blocked  

### Operator examples

| Fact key | Product role | Destination (v1 audit) |
|----------|--------------|------------------------|
| `op.snapshot.companyName` | Display identity | `Operator Setup - Profile & Positioning` → `company_name` |
| `op.snapshot.companyDescription` | Explorer summary | `Operator Setup - Profile & Positioning` → `companyDescription` |
| `op.markets.regionsSupported` | Regional presence | `Operator Setup - Platform & Markets` → `specificMarkets` |
| `op.brand.familiesOperated` | Brand relationships | `Operator Setup - Profile & Positioning` → `Brand Families Operated` |
| `op.platform.offeredServices` | Capabilities | `Operator Setup - Governance, Delivery & Diligence` → `Offered Services` |
| `op.snapshot.primaryServiceModel` | Service model | `Operator Setup - Profile & Positioning` → `primaryServiceModel` |

### Brand examples

| Fact key | Product role | Destination |
|----------|--------------|-------------|
| `be.identity.brandName` | Identity | Brand Basics → `Brand Name` |
| `be.positioning.summary` | Positioning | Brand Basics → `Brand Positioning` + slot `overview.relative_positioning` |
| `be.footprint.geoIntro` | Footprint narrative | Presentation slot `footprint.geo_intro` |

Brand facts often target **presentation slots** (staging-friendly) before Basics columns.

---

## 4. Publishing modes

### Mode A — Evidence Only

- Fact remains in Extracted Facts (+ optional Published overlay read path).  
- Supports trust chip, audit, gap questions, outreach context.  
- **Does not** write Setup / presentation product fields.

**Default in v1** for: unsupported keys, inference-risk keys, registry gaps.

### Mode B — Suggested Field Update

- Approved fact + approved source produce a **proposed value** and destination.  
- Steward reviews diff (live vs proposed).  
- **Required when:** destination field is non-empty, identity field populated, or select-option validation needed.

### Mode C — Controlled Publish

Allowed only when **all** are true:

- Source **Approved for Explorer Use = Yes**  
- Fact **Approved** or **Edited**  
- Governance allows platform display  
- Destination field **blank** OR explicit overwrite approval  
- Field key in registry publish scope  
- **Not** a governance / attestation / scoring field  
- Select options validated (for `singleSelect` / `multipleSelects`)

**v1:** Classified in audit only — no write path implemented.

---

## 5. Safety rules

| Rule | Enforcement |
|------|-------------|
| Never overwrite **Company Validated** data | Blocked destination list |
| Never overwrite **Company Reviewed** without explicit approval | Blocked + human gate |
| Never overwrite non-empty curated fields by default | → Suggested Update |
| Never publish Pending / Rejected / quarantined facts | Excluded from audit mappings |
| Never publish from unapproved sources | `source_not_explorer_approved` blocker |
| Never publish unsupported field keys | Blocked or Evidence Only |
| Preserve source/fact provenance | Required on any future write |
| Prefer staging / Published overlay before production fields | Policy per key |
| Do not change scoring from new facts | BAS/OAS/OCS out of scope |
| Do not infer fit/suitability from weak evidence | `op.dealFit.*` blocked |

---

## 6. Destination field audit

Run per entity:

```bash
npm run approved-intelligence-field-publishing-audit -- --entity-type operator --target-rec-id rec...
```

Report columns per approved fact:

- fact key, approved value  
- destination table + field  
- field exists (from build sheet / registry)  
- safe to write (classification)  
- staging-only flag  
- live value populated  
- publish mode + blockers  

Full registry keys: `listRegistryFieldKeys()` in `approved-intelligence-field-publishing.js`.

---

## 7. GHL Hoteles mapping example

**Operator:** `reciI2tYQBfMoMK9G` — governance **Company Published** / AI-Assisted Profile.

| Approved fact | Recommended mode | Rationale |
|---------------|------------------|-----------|
| `op.snapshot.companyName` | **Suggested** if `company_name` populated | Identity — no blind overwrite |
| `op.snapshot.companyDescription` | **Suggested** or **Controlled** if blank | Safe summary enrichment |
| `op.markets.regionsSupported` | **Suggested** | Regional text; validate vs live `specificMarkets` |
| `op.brand.familiesOperated` | **Suggested** | multipleSelects — option validation |
| `op.platform.offeredServices` (events/MICE) | **Suggested** | multipleSelects; narrow approved subset only |
| Pending facts (`primaryServiceModel`, guest services) | **Evidence only / excluded** | Not approved — no publish |

---

## 8. Hotel Equities mapping example

**Operator:** `recWPKu5laVZxsvpn` — 5 approved website facts; PDF facts manual-curation only.

| Fact | Mode | Notes |
|------|------|-------|
| `op.snapshot.companyDescription` | Suggested / Controlled if blank | Website-sourced; strong |
| `op.markets.regionsSupported` | Suggested | CALA scope — verify vs live |
| `op.platform.offeredServices` | Suggested | Select validation |
| `op.brand.familiesOperated` | Suggested | Brand list from site |
| PDF-derived Pending facts | **Evidence only** | Noisy deck — manual curation plan |
| `op.ownerValueProposition` | **Evidence only** | Unsupported registry key in v1 |

---

## 9. Brand example (Curio / Kimpton)

**Curio** (`receQkxgjlezsc1xg`): sparse identity facts — remain **evidence + governance**; positioning slots **suggested** only after substantive approved facts.

**Kimpton** (`recCKuXCmGvxHPfb3`): company-materials path — `be.positioning.*` → Brand Basics / presentation slots as **suggested**; never overwrite parent-company identity without review.

Brand differs from operator: more **presentation slot** targets; fewer Operator Setup child tables.

---

## 10. Platform usage

| Module | How to consume approved intelligence |
|--------|--------------------------------------|
| **Explorer** | Trust chip from governance; field values from Setup + optional PI overlay |
| **Alignment snapshots** | Supporting signals only — not automatic conclusions |
| **Deal Readiness** | Gap questions from missing/weak facts |
| **Outreach prep** | Context bullets from approved facts (Helena intake) |
| **Matching** | Future — requires explicit scoring rules |

---

## 11. Proposed v1 implementation

| Module | Role |
|--------|------|
| `lib/partner-intelligence/approved-intelligence-field-publishing.js` | Destination resolve, classify, audit builder |
| `scripts/approved-intelligence-field-publishing-audit.mjs` | Read-only CLI |
| `scripts/test-approved-intelligence-field-publishing.mjs` | Classification tests |

**Reports:** `reports/approved-intelligence-field-publishing-audit.{md,json}` and per-entity `reports/approved-intelligence-field-publishing-audit-<recId>.{md,json}`

**Not in v1:** apply/write, Published row auto-create, Setup PATCH, UI changes.

**Next (v1 suggestions):** [approved-intelligence-field-suggestions-v1.md](./approved-intelligence-field-suggestions-v1.md) — `npm run approved-intelligence-field-suggestions`

**v2 candidates:** staging column family, steward approve UI, batch suggest queue integration.

---

## Change Impact

| Tier | **Low** — read-only audit; no schema or write paths |
| Rollback | Remove package script + lib module |

## Regression checklist

- [ ] `npm run test:approved-intelligence-field-publishing`
- [ ] Audit GHL + Hotel Equities — no Airtable writes
- [ ] Confirm `--apply` rejected
- [ ] Company Validated unchanged after audit runs
