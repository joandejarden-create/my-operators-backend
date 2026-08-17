# Curio Partner Intelligence Package Integrity Cleanup Plan

**Date:** 2026-07-06  
**Status:** Investigation complete — **do not apply stewardship or publish** until cleanup.  
**Target:** Curio Collection by Hilton — Brand Setup `receQkxgjlezsc1xg`

> **Authority:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md), [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md)  
> **Inputs (read-only):** `reports/partner-intelligence-stewardship-package.{md,json}` (2026-07-06), `reports/partner-intelligence-publish-readiness.{md,json}` (2026-07-06), live PI table read for fact-by-source analysis

---

## Current Status

| Item | Value |
|------|-------|
| **Target record** | `receQkxgjlezsc1xg` — Curio Collection by Hilton |
| **Source count** | 15 (all linked to Curio brand) |
| **Fact count** | 144 (0 approved; **24 rejected/quarantined**, 120 pending candidates) |
| **Facts by source** | `recIH5lyY8MASnfrp` (Mexico FDD): **102** · `rec6opP76pLDVjDuP` (points guide): **36** · `recy2pyEahF9UUsEk` (US FDD): **6** |
| **Publish readiness** | **Blocked** — not eligible |
| **Stewardship projection** | Still **not eligible** after all sources patched + top 8 facts |

### Blockers (readiness + stewardship)

| Blocker | Status |
|---------|--------|
| `no_approved_facts` | Yes — 0 approved |
| `approved_for_explorer_use_no` | Yes — all 15 sources |
| `conflict:mixed_company_and_public_source_origins` | Yes — **hard blocker** |
| Wrong-brand extracted values | Yes — **integrity failure** (see below) |

### Why Curio is not safe to publish yet

1. **No trustworthy identity facts** — all `be.identity.*` rows are Kimpton/IHG values or absent; zero Curio/Hilton identity facts pass QA.
2. **Mexico FDD extraction batch is contaminated** — 102 facts from one source carry Kimpton Hotels / IHG Hotels & Resorts values (wrong extraction template or cross-brand run).
3. **Mixed source origins** — Brand Provided + Public Web + Other in one governance package triggers `detectSourceOriginConflict()`; profile-governance publish must not proceed until steward classifies sources.
4. **Bulk stewardship would amplify harm** — the generalized assistant’s top-scored facts are the **wrong** identity rows; auto-approve would encode false brand/parent data.
5. **12 of 15 sources have no facts** — package assembly is source-heavy, fact-light, and includes duplicate captures and non-Curio Hilton corporate content (Honors program, APAC brochure).

---

## Critical Data Quality Issues

### Wrong-brand facts (red flags)

| Fact ID | Field | Extracted value | Source |
|---------|-------|-----------------|--------|
| `recHXBcC5nZD4yx6b` | `be.identity.brandName` | **Kimpton Hotels** | `recIH5lyY8MASnfrp` |
| `recFC00O4aDSA2l8e` | `be.identity.parentCompany` | **IHG Hotels & Resorts** | `recIH5lyY8MASnfrp` |

**Why this is critical:** Curio Collection by Hilton is a **Hilton** soft brand. Kimpton is also Hilton, but **not Curio**. IHG is a **competitor parent**. Publishing or approving these values would misstate brand identity on Explorer and undermine trust-label credibility.

**Live scan (read-only, 2026-07-06):** **24 of 144 facts** match Kimpton/IHG/wrong-parent patterns — **all 24 trace to `recIH5lyY8MASnfrp` (2025 Mexico Curio FDD)**. No `be.identity.*` fact contains “Curio” or “Hilton” as the extracted value.

### Likely causes

| Hypothesis | Evidence |
|------------|----------|
| **Wrong extraction run / template** | Mexico FDD facts repeat Kimpton/IHG boilerplate (`IHG One Rewards`, “IHG development brochure…”) — pattern matches Kimpton pilot extraction, not Curio. |
| **Source linked to Curio but content/run from another brand** | Source row title says Mexico Curio FDD; facts are wrong-brand — file may be correct but extraction pipeline reused wrong registry/run. |
| **Package assembly too broad** | 15 sources include Hilton Honors, third-party travel blogs, competitor comparisons — not Curio-specific governance evidence. |
| **Recommendation scoring blind to brand match** | Stewardship assistant scores `be.identity.*` highly on evidence + “Directly Stated” without validating value matches target brand. |
| **Duplicate source rows** | Two fact sheets, two Honors overviews, two press captures, two APAC brochures — increases noise and origin conflict. |

### Secondary issues

- **US Curio FDD facts (`recy2pyEahF9UUsEk`)** — 6 duplicate `be.footprint.globalHotels` = `"06"` (low integrity; needs re-extract).
- **Points guide facts (`rec6opP76pLDVjDuP`)** — 36 facts, mostly `"Not confirmed in available sources."` — not governance-ready.
- **Unlinked duplicate packages** (readiness audit) — `recA7VOMWhmTtiklj`, `recW4sJlpnCdZAZJ0` — Fact Sheet / FDD rows missing entity link (separate cleanup).

---

## Source Inventory

| Record ID | Title | Source type | Source origin | Status | Explorer use | Quality | Stale? | Linked brand | Suspected group | Recommendation |
|-----------|-------|-------------|---------------|--------|--------------|---------|--------|--------------|-----------------|----------------|
| `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | FDD | Brand Provided | Extracted | No | High | No | Curio | company / regulatory | **Quarantine** — facts contaminated; re-extract or reject all 102 facts before any approve |
| `recy2pyEahF9UUsEk` | 2025 US Curio FDD | FDD | Brand Provided | Extracted | No | High | No | Curio | company / regulatory | **Keep** (company-materials path) — re-extract; do not approve current 6 footprint facts |
| `recL1qfHCOAUZr9Rz` | Curio Collection Fact Sheet May 2026 | Other | Brand Provided | Extracted | No | High | No | Curio | company materials | **Keep** — extract/review; primary Curio positioning source candidate |
| `recMuN9bR1doJ3gjN` | Curio Collection Fact Sheet May 2026 | Website Capture | Brand Provided | Extracted | No | High | No | Curio | company materials | **Duplicate** of above — merge/dedupe; one canonical row |
| `recR0bSGVPmzuVSnl` | Hilton Develop APAC Brochure | Other | Brand Provided | Extracted | No | High | No | Curio | company materials (Hilton corp) | **Manual review** — Hilton parent context; may support development model, not Curio identity |
| `recstXfpXJmYNIrnz` | Hilton Develop APAC Brochure | Development Brochure | Public Web | Extracted | No | Medium | No | Curio | public / company mix | **Exclude from governance publish** — duplicate + Public Web origin |
| `rec9NH6nBLsHUlBqr` | Hilton Honors Program Overview | Other | Brand Provided | Extracted | No | High | No | Curio | Hilton corporate / loyalty | **Exclude from governance publish** — not Curio-specific |
| `recm0BO5Y7tyR732T` | Hilton Honors Program Overview | Website Capture | Brand Provided | Extracted | No | High | No | Curio | Hilton corporate / loyalty | **Exclude** — duplicate Honors overview |
| `recFqF8CETGBerPXC` | Hilton Honors 2026 Program Changes Press Release | Press Release | Public Web | Extracted | No | Medium | No | Curio | press / news | **Exclude from governance publish** |
| `recnzI8rOX2xjpchb` | Hilton Honors 2026 Program Changes Press Release | Other | Brand Provided | Extracted | No | High | No | Curio | press / news | **Exclude** — duplicate press capture |
| `rectS4eaOgRfxgsKE` | Hilton Honors 2026 Press Release full capture | Other | Brand Provided | Extracted | No | High | No | Curio | press / news | **Exclude** |
| `recslDSN2UNtENJTy` | Hilton Honors Points and Miles | Website Capture | Public Web | Extracted | No | Medium | No | Curio | public / Hilton corporate | **Exclude from governance publish** |
| `rec6opP76pLDVjDuP` | Best Hilton Hotels Points Value Guide 2026 | Other | Other | Extracted | No | Medium | No | Curio | third-party analysis | **Exclude** — wrong link suspected; 36 low-value facts |
| `recwCTH346cX1FRWu` | Prince of Travel Hilton Honors 2026 Analysis | Other | Other | Extracted | No | Medium | No | Curio | third-party analysis | **Exclude** |
| `recOlxkapaiqtBe5R` | MT Luxury Marriott vs Hilton vs Hyatt Comparison | Other | Other | Extracted | No | Medium | No | Curio | third-party analysis | **Exclude** — competitor comparison, not Curio evidence |

**Summary grouping (for governance publish package):**

| Group | Count | Use in profile-governance publish |
|-------|-------|-----------------------------------|
| Curio FDD / fact sheet (clean after re-extract) | 3–4 | **Yes** (company-materials path) |
| Hilton corporate / Honors / press | 7 | **No** — internal reference only |
| Third-party / comparison | 3 | **No** |
| Mexico FDD (contaminated facts) | 1 | **Hold** until facts quarantined |

---

## Fact Integrity Review

### Highest-priority contaminated facts (reject or quarantine)

| Fact ID | Field key | Extracted value | Source ID | Source title | Issue | Recommended action |
|---------|-----------|-----------------|-----------|--------------|-------|-------------------|
| `recHXBcC5nZD4yx6b` | `be.identity.brandName` | Kimpton Hotels | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong brand | **Reject** (after human confirm) |
| `recFC00O4aDSA2l8e` | `be.identity.parentCompany` | IHG Hotels & Resorts | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong parent (competitor) | **Reject** |
| `recHb2nUe12R5JQeg` | `be.identity.parentCompany` | IHG Hotels & Resorts | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Duplicate wrong parent | **Reject** |
| `recJ38rLNWit8qUyl` | `be.identity.parentCompany` | IHG Hotels & Resorts | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Duplicate wrong parent | **Reject** |
| `recJbyJUq1wQN9xlE` | `be.identity.parentCompany` | IHG Hotels & Resorts | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Duplicate wrong parent | **Reject** |
| `rechKRzATphNpSss6` | `be.identity.parentCompany` | IHG Hotels & Resorts | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Duplicate wrong parent | **Reject** |
| `recoGK5NYwtQz6mIP` | `be.identity.parentCompany` | IHG Hotels & Resorts | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Duplicate wrong parent | **Reject** |
| `recdpj2EEkRKxTTzi` | `be.identity.brandName` | Kimpton Hotels | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong brand | **Reject** |
| `recgxO5Plpxa8kzpR` | `be.identity.brandName` | Kimpton Hotels | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong brand | **Reject** |
| `recupFJcPebGoab6V` | `be.identity.brandName` | Kimpton Hotels | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong brand | **Reject** |
| `recxFG4xuKHrQknir` | `be.identity.brandName` | Kimpton Hotels | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong brand | **Reject** |
| `recxlbHEpHAYsFvjG` | `be.identity.brandName` | Kimpton Hotels | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong brand | **Reject** |
| `rec8c7ZxMHcPlP9Y4` | `be.loyalty.programName` | IHG One Rewards | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong loyalty program | **Reject** |
| `recFH4isphK8DBnQE` | `be.meta.overallSourceConfidence` | …IHG development brochure… | `recIH5lyY8MASnfrp` | 2025 Mexico Curio FDD | Wrong-brand meta text | **Reject** |

**Batch action:** Quarantine **all 102 facts** from `recIH5lyY8MASnfrp` pending re-extraction from the Mexico FDD with Curio-specific run ID and human spot-check.

### Other concerning facts

| Fact ID | Field | Value | Source | Issue | Action |
|---------|-------|-------|--------|-------|--------|
| `recD61KjKSlrboU76` (and 5 dupes) | `be.footprint.globalHotels` | `06` | `recy2pyEahF9UUsEk` | Nonsense duplicate footprint | **Re-extract** from US FDD |
| 36 facts on `rec6opP76pLDVjDuP` | various | Not confirmed… | Points guide | No evidence | **Reject** or unlink source from Curio |

### Facts safe to approve later

**None identified** in current package. After re-extract from US FDD + fact sheet, expect candidates:

- `be.identity.brandName` → Curio Collection by Hilton (or equivalent)
- `be.identity.parentCompany` → Hilton (or Hilton Worldwide Holdings)
- `be.positioning.summary` / `be.positioning.tagline` from fact sheet

---

## Origin Conflict Resolution

### What triggers the blocker

`detectSourceOriginConflict()` in `profile-governance-publish-readiness.js` flags when the package includes both:

- **Company-provided** origins (`Brand Provided`, `Operator Provided`), and  
- **Public** origins (`Public Web`, etc.)

Curio package currently has **9 Brand Provided**, **3 Public Web**, **3 Other** sources in one rollup.

### Options

| Option | Description | Pros | Cons |
|--------|-------------|------|------|
| **A — Split paths** | Company-materials package vs public/regulatory package | Clear governance labels per path | Two publish workflows; may confuse owners |
| **B — Exclude public/third-party from publish package** | Use FDD + fact sheet only for governance publish; keep Honors/press/blog as internal | Matches Kimpton/Arbor pilot pattern; resolves conflict | Requires unlinking or steward “exclude” decision on 8+ sources |
| **C — Keep mixed; downgrade label** | Publish as Source-Informed only | Faster | **Does not fix wrong-brand facts**; still misleading |

### Recommendation: **Option B** (with selective re-extract)

1. **Profile-governance publish package** = Curio-specific **company materials only**: US FDD (`recy2pyEahF9UUsEk`), one canonical fact sheet (`recL1qfHCOAUZr9Rz` or `recMuN9bR1doJ3gjN`), optionally Mexico FDD **after** clean re-extract.
2. **Exclude** Hilton Honors, press releases, third-party blogs, competitor comparisons, and duplicate captures from the publish rollup (leave in Source Library for internal research if needed; do not approve for Explorer Use until Curio-relevant).
3. After cleanup, external label will likely be **AI-Assisted Profile** (company FDD/fact sheet) or **Source-Informed Profile** if any vetted public regulatory source remains — never Company-Validated / Company-Reviewed.

Option A is a valid follow-on if product wants separate chips for “company PDF” vs “public FDD filing” — not required for first Curio publish.

---

## Recommended Cleanup Sequence

1. **Do not publish** profile governance for Curio.
2. **Quarantine wrong-brand facts** — `npm run quarantine-curio-pi-facts -- --dry-run` (rule-detected contaminated rows); after founder review, `--apply --approve-curio-quarantine`. Remaining facts from `recIH5lyY8MASnfrp` (78 without Kimpton/IHG text match) need manual batch review or re-extract.
3. **Review source links** — confirm Curio brand link is intentional for each of 15 rows; unlink or reassign third-party sources (`rec6opP76pLDVjDuP`, `recwCTH346cX1FRWu`, `recOlxkapaiqtBe5R`) if not Curio-specific.
4. **Dedupe sources** — pick one canonical fact sheet, one Honors overview, one APAC brochure, one press release; mark duplicates `Stale` or merge per PI workflow.
5. **Re-extract** from US FDD + fact sheet with Curio brand registry / run ID; verify identity fields manually — see [curio-clean-reextraction-plan.md](./curio-clean-reextraction-plan.md).
6. **Classify sources** — approve Explorer Use **only** for clean company-materials subset (likely 2–3 sources, not 15).
7. **Approve a small set of clean facts** (3–6 identity/positioning) after evidence review — never Kimpton/IHG values.
8. **Re-run** `npm run audit-partner-intelligence-publish-readiness` — confirm no origin conflict and `no_approved_facts` cleared.
9. **Dry-run** `npm run steward-partner-intelligence` and `npm run publish-partner-intelligence-profile-governance -- --dry-run` only if blockers clear.

---

## Proposed Safe Governance Outcome

**After cleanup only** — conservative external display per [governance-read-path-trust-label-plan.md](./governance-read-path-trust-label-plan.md):

| Scenario | Internal validation (Setup) | External chip |
|----------|----------------------------|---------------|
| Clean company FDD + fact sheet | Company Published (internal) | **AI-Assisted Profile** · Source Basis: Company Materials |
| Vetted public FDD filing only | Source-Informed | **Source-Informed Profile** · Source Basis: Reviewed Sources |

**Never:** Company-Validated Profile, Company-Reviewed Profile, or raw “Company Published” externally.

**Region:** Global Reference (or market-specific after steward review).

---

## Apply Safety Notes

- **Do not** use bulk apply for all 15 sources.
- **Do not** approve facts with Kimpton, IHG, or “IHG One Rewards” values.
- **Do not** approve the mixed-source package until origin conflict is resolved.
- **Do not** set Company Validated or Company Validation Date.
- **Do not** set External Display Status / Show Trust Label on Brand Basics directly.
- **Do not** trust stewardship **recommended fact IDs** for Curio without value validation — scoring does not yet check brand-name match.

---

## Related Commands

```bash
npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run
npm run audit-partner-intelligence-publish-readiness
npm run quarantine-curio-pi-facts -- --dry-run
```

**Contaminated fact quarantine (dry-run default):** `npm run quarantine-curio-pi-facts` — flags/rejects wrong-brand facts from Mexico FDD source `recIH5lyY8MASnfrp`. Apply requires `--apply --approve-curio-quarantine`. **24 facts quarantined (2026-07-06).**

**Clean re-extraction plan (next step):** [curio-clean-reextraction-plan.md](./curio-clean-reextraction-plan.md) — narrow company-material sources only (`recy2pyEahF9UUsEk`, `recL1qfHCOAUZr9Rz`); no publish until clean identity/positioning facts approved.

**Clean re-extraction (dry-run default):** `npm run curio-clean-reextract -- --dry-run` → `reports/curio-clean-reextract.{md,json}`. Apply requires `--apply --approve-curio-clean-reextract`. Allowlist: US FDD + fact sheet only.

**Extraction context audit:** [curio-extraction-context-audit.md](./curio-extraction-context-audit.md) — Kimpton/IHG blind fallback fixed in code (2026-07-06).

**Publish readiness (2026-07-06):** Eligibility uses **approved Explorer-use publish scope** only — 14 non-approved sources are diagnostics; Curio eligible on US FDD + 2 identity facts after stewardship apply.

**No apply command** for profile governance publish until founder reviews sparse fact set and runs publish dry-run.

---

## Follow-up engineering (optional, not in scope)

- Add brand-name / parent-company validation to stewardship fact recommendations.
- Add extraction run provenance audit (run ID → brand registry).
- Report script: `audit-pi-package-integrity.mjs` (future) — flag cross-brand values before stewardship.

---

## Change Impact

| Tier | Classification |
|------|----------------|
| This document | **Low** — documentation only |
| Future cleanup actions | **High** — PI fact reject/re-extract, source linkage |

**Rollback:** N/A (read-only investigation).
