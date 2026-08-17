# Hotel Equities — PDF Manual Curation Plan

**Date:** 2026-07-06  
**Status:** Planning — **no automatic PDF extract apply**; steward facts by hand or curated override  
**Operator:** Hotel Equities (CALA) — `recWPKu5laVZxsvpn`

> **Authority:** [hotel-equities-pdf-enrichment-plan.md](./hotel-equities-pdf-enrichment-plan.md), [hotel-equities-extraction-plan.md](./hotel-equities-extraction-plan.md), `reports/hotel-equities-extract.{md,json}`

---

## 1. Current governance status

| Item | Value |
|------|-------|
| **Operator Setup - Master** | `recWPKu5laVZxsvpn` |
| **Publish readiness** | **Eligible / `no_op`** (website package aligned) |
| **Validation Status** | Company Published |
| **Source Type** | Company Website |
| **Data Confidence Level** | Medium |
| **External chip** | AI-Assisted Profile · Source Basis: Company Materials |
| **Company Validated** | **No** (unchanged) |
| **Publish scope today** | 3 approved website sources; 5 approved facts |
| **PDF sources** | Registered (`Captured`); **not** in publish scope; **not** Explorer-approved |

**Approved website facts (do not duplicate without additive value):**

| Fact ID | Field | Notes |
|---------|-------|-------|
| `rec4OkNp3HErir1Tm` | `op.snapshot.companyName` | Hotel Equities |
| `rec5ZV7hxlyZz3eRk` | `op.snapshot.companyDescription` | CALA page narrative |
| `recg9JSrZm9gmFKcN` | `op.snapshot.companyDescription` | Home narrative |
| `recQEsdNe6Z6yYl7R` | `op.markets.regionsSupported` | From website |
| `recDasPN4e1SOJOUa` | `op.snapshot.primaryServiceModel` | Hotel Management |

Governance re-publish is **not required** while PDF enrichment remains additive review only.

---

## 2. PDF source IDs and titles

| Source ID | Title | Local file | Status | Explorer use |
|-----------|-------|------------|--------|--------------|
| `recxdPFckVzA3ckmN` | HE CALA Marketing Presentation March 2026 | `Hotel Equities CALA/HE CALA Marketing Presentation  March 2026.pdf` | Captured | No |
| `rectqBTiGkq3hUlXa` | Caribbean & Latin America Hospitality Company — Hotel Equities | `Hotel Equities CALA/Caribbean & Latin America Hospitality Company _ Hotel Equities.pdf` | Captured | No |

---

## 3. Why automatic PDF apply is not recommended yet

PDF enrichment dry-run (`npm run hotel-equities-extract -- --dry-run --source-group pdf`, 2026-07-06):

| Metric | Result |
|--------|--------|
| Proposed facts | 4 |
| Skipped | 12 |
| Unsupported keys | 2 (`op.ownerValueProposition`, `op.operatingModel`) |
| Airtable writes | 0 |
| `op.platform.offeredServices` | **Not cleanly extracted** (deck slide noise / gap copy) |
| `op.snapshot.companyDescription` | **Noisy or gap** on both PDFs |
| Publish-scope strength | **Not strong enough** (no clean offered-services fact) |

**Root causes:**

1. **Marketing deck layout** — slide statistics and F&B portfolio bullets pollute inferred fields (`pdf_deck_slide_noise`).
2. **Low-confidence inference** — several survivors are `Inferred from Context` / Low confidence.
3. **Duplicate risk** — `op.snapshot.primaryServiceModel` auto-hit duplicates approved website fact `recDasPN4e1SOJOUa`.
4. **Primary enrichment gap** — `op.platform.offeredServices` was the main PDF goal; automatic path did not produce a steward-ready value.
5. **Registry limits** — owner value proposition / operating model are not supported extraction keys.

Applying the 4 proposed rows would create **Pending** facts that still need heavy manual edit or rejection — higher risk than curated creation.

---

## 4. Candidate facts worth manual curation

### P0 — `op.platform.offeredServices` (highest value; manual only today)

| Source | Steward action |
|--------|----------------|
| `recxdPFckVzA3ckmN` | Open marketing deck; extract **bullet list** of CALA / HE management services (e.g. third-party management, F&B, development support) from services/platform slides — **not** portfolio stat slides. |
| `rectqBTiGkq3hUlXa` | Secondary; overview PDF may not list services — use only if explicit service bullets exist. |

**Target shape:** Short comma-separated or sentence list suitable for Explorer “Offered Services” — directly stated, with slide/page evidence.

### P1 — `op.brand.familiesOperated` (strong auto candidate; still curate)

| Source | Dry-run value | Manual note |
|--------|---------------|-------------|
| `recxdPFckVzA3ckmN` | Marriott, Hilton, IHG, Choice | **Prefer** if deck shows franchise partner brands on a dedicated slide. **Additive** vs website if website only listed Marriott, Hilton. |
| `rectqBTiGkq3hUlXa` | Hilton only | **Weaker alone** — skip unless corroborating a CALA-specific Hilton relationship; do not replace marketing-deck list. |

**Curated recommendation:** One fact from **marketing deck** with full brand list; omit overview-PDF Hilton-only row unless it adds CALA-specific context.

### P0 — `op.markets.regionsSupported` (curate selectively)

| Source | Dry-run value | Manual note |
|--------|---------------|-------------|
| `rectqBTiGkq3hUlXa` | CALA mission / Caribbean & Latin America footprint | **Prefer** — substantive, CALA-specific; may **extend** (not replace) website `recQEsdNe6Z6yYl7R` if value is clearer. |
| `recxdPFckVzA3ckmN` | “fast-growing hospitality markets” (leadership quote) | **Avoid** — vague; not a region list. |

**Curated recommendation:** Manual value such as `Caribbean, Latin America` or `CALA` with exact quote from overview PDF opening section — not leadership boilerplate.

### P0 — `op.snapshot.primaryServiceModel` (only if stronger than website)

| Rule | Action |
|------|--------|
| Website approved | `Hotel Management` (`recDasPN4e1SOJOUa`) |
| PDF auto-extract | Duplicate — **do not create** unless PDF states a **more specific** model (e.g. “third-party hotel management for owners” with evidence). |

### P0 — `op.snapshot.companyDescription` (only if cleaner than website)

| Rule | Action |
|------|--------|
| Website | Two approved descriptions (home + CALA) already in publish scope |
| PDF auto | Slide stats / gap copy — **do not apply** |
| Manual | Only if a **single concise CALA division paragraph** from overview PDF is clearly better than `rec5ZV7hxlyZz3eRk` — otherwise skip |

---

## 5. Candidate facts to avoid

| Avoid | Reason |
|-------|--------|
| Vague regional claims | e.g. “fast-growing hospitality markets” without named regions |
| Slide-stat fragments | Hotel counts, F&B outlet counts, “WHY HE CALA” deck headers |
| Duplicate descriptions | Second/third `companyDescription` unless materially better |
| Duplicate primary service model | Same as `Hotel Management` without added specificity |
| `op.ownerValueProposition` | **Unsupported** in explorer field registry |
| `op.operatingModel` | **Unsupported** in explorer field registry |
| Gap / “Not confirmed” placeholders | Never publish |
| Low-confidence inferred blobs | Long pasted deck text without editorial cleanup |
| Marketing superlatives alone | “world-class”, “leading”, etc. without operational detail |

---

## 6. Manual review checklist

For each candidate fact, complete before creating or approving an Extracted Fact row:

| # | Check | Record |
|---|-------|--------|
| 1 | **Exact quote / evidence** | Verbatim sentence or bullet from PDF |
| 2 | **Source page / slide** | e.g. “Marketing deck p.4” or “Overview PDF p.1” |
| 3 | **Source record** | `recxdPFckVzA3ckmN` or `rectqBTiGkq3hUlXa` |
| 4 | **Field key** | Registry key (e.g. `op.platform.offeredServices`) |
| 5 | **Proposed value** | Steward-edited Explorer-safe text (not raw extract blob) |
| 6 | **Extraction type** | `Directly Stated` when quote-supported; `Inferred` only with caution |
| 7 | **Duplicate or additive?** | Compare to approved website facts table above |
| 8 | **Suitable for Explorer?** | Owner-facing, factual, CALA-relevant |
| 9 | **Human Review Status** | Create as **Pending**; approve only after checklist pass |
| 10 | **Public visibility** | `Public` for Explorer-bound facts |

**Suggested manual fact set (2–4 rows after review):**

1. `op.platform.offeredServices` — from marketing deck (if found on review) — **source `recxdPFckVzA3ckmN`**
2. `op.brand.familiesOperated` — Marriott, Hilton, IHG, Choice (if slide confirms) — **source `recxdPFckVzA3ckmN`**
3. `op.markets.regionsSupported` — CALA-focused region string — **source `rectqBTiGkq3hUlXa`**

Skip overview-PDF `Hilton`-only brand row unless it adds unique CALA evidence.

---

## 7. Recommended next Airtable action

### Option A — Manual fact creation (recommended first)

After PDF review in Airtable **Partner Intelligence - Extracted Facts**:

1. Create **2–4 Pending** rows linked to `recWPKu5laVZxsvpn` and the relevant **Source Record**.
2. Map fields per [partner-source-library-airtable-fields.md](../partner-source-library-airtable-fields.md) / `MAP_PARTNER_FACT`.
3. Set **Human Review Status** = Pending; do not bulk-approve.
4. Patch source **Status** → `Extracted` only for PDFs that contributed approved facts (manual or after curated script).

### Option B — Curated override script (future)

Repo does **not** yet ship HE PDF curated overrides. If implemented:

- New allowlist file e.g. `fixtures/hotel-equities-pdf-curated-facts.json`
- Narrow script dry-run: validate evidence + field keys + source IDs
- Apply creates Pending facts only (same guardrails as `hotel-equities-extract`)
- **Dry-run first**; founder approval before apply

**Do not run** `npm run hotel-equities-extract -- --apply --source-group pdf` until overrides exist or founder explicitly accepts the 4 noisy proposed rows.

---

## 8. Recommended source stewardship

| Rule | Rationale |
|------|-----------|
| **Do not** approve PDF for Explorer use until ≥1 useful fact from that PDF is approved | Prevents empty publish-scope inflation |
| **Prefer** approving only `recxdPFckVzA3ckmN` if marketing deck yields offered services + brand list | Overview PDF may add only regions |
| **Defer** approving `rectqBTiGkq3hUlXa` if only redundant Hilton brand hit | Avoid dual-source noise |
| Keep **Approved for Extraction** = No until manual facts exist or curated apply is approved | Extraction is not auto-gated but stewardship discipline |
| Advance **Status** Captured → Approved → Extracted in step with fact review | Align source status with evidence use |

**Suggested steward command after manual facts exist:**

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

Apply source patches only with explicit `--approve-source-ids` for PDFs that have approved facts.

---

## 9. Governance recommendation

| Action | Recommendation |
|--------|----------------|
| Re-publish profile governance | **Not needed** unless PDF facts materially improve substance (e.g. clean `offeredServices` + approved for Explorer) |
| Evidence Notes | May update on future publish to mention PDF sources — optional |
| Confidence Level | May remain **Medium** until ≥3 substantive approved facts include PDF contribution |
| Validation Status | Stays **Company Published** if PDFs are company materials and added to publish scope later |
| Company Validated | **Never** from PI/PDF path |
| External Display Status | **Unchanged** — already Show Trust Label from website package |

Re-run publish dry-run only after PDF sources are Explorer-approved **and** new facts approved:

```bash
npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run
```

Expect `no_op` or minor Evidence Notes change — not a validation downgrade.

---

## 10. Exact next implementation option

| Option | Steps | When |
|--------|-------|------|
| **A — Manual Airtable facts** | Review PDFs → checklist → 2–4 Pending facts → steward approve subset → optional source Explorer approval → re-audit readiness | **Now** (lowest risk) |
| **B — Curated override script** | Add `fixtures/hotel-equities-pdf-curated-facts.json` + dry-run writer → founder review → apply Pending facts only | After manual review defines exact values/evidence |

**Recommended path:** **Option A** for first PDF enrichment facts; Option B only if the same values will be reused or batch-validated.

---

## Commands (read-only)

```bash
npm run hotel-equities-extract -- --dry-run --source-group pdf
npm run audit-partner-intelligence-publish-readiness
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

---

## Change Impact

| Tier | **Low** — documentation and manual stewardship only |
| Airtable | No changes until founder creates/approves facts manually |

## Regression checklist

- Website governance remains `no_op`.
- Do not approve PDF Explorer use without approved PDF-backed facts.
- Do not set Company Validated.
- Do not run PDF extract `--apply` without curated values.
