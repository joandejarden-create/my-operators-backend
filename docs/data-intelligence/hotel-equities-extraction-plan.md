# Hotel Equities — Extraction Plan

**Date:** 2026-07-06  
**Status:** HTML normalization **fixed** — read-only preview clean; **apply still via narrow script only**  
**Operator:** Hotel Equities (CALA) — `recWPKu5laVZxsvpn`

> **Authority:** [hotel-equities-source-capture-plan.md](./hotel-equities-source-capture-plan.md), [hotel-equities-pi-production-plan.md](./hotel-equities-pi-production-plan.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md)

---

## Executive Summary

| Question | Answer |
|----------|--------|
| **Sources in scope** | 3 captured official website HTML rows (allowlist) |
| **Suitable for extraction?** | **Yes** — local `.html`/`.htm` now parsed with cheerio (same path as URL fetch) |
| **Safe apply path today?** | **Yes (narrow script)** — `npm run hotel-equities-extract` dry-run default; apply requires explicit flags |
| **Read-only preview** | `node scripts/hotel-equities-extract-preview.mjs` (legacy) · **`npm run hotel-equities-extract -- --dry-run`** (narrow) |
| **Recommended next step** | Review dry-run report → founder approval → `--apply --approve-hotel-equities-extract` |

---

## 1. Source Records in Scope

| Priority | Source ID | Title | URL | Local file | Status | Explorer use |
|----------|-----------|-------|-----|------------|--------|--------------|
| P0 | `rectG9wdsAeL7u0FG` | Hotel Equities home | https://www.hotelequities.com/ | `Hotel Equities/website/Hotel Equities home.html` | Captured | No |
| P0 | `rec9FSzLhaLPcPvtv` | Hotel Equities Services | https://www.hotelequities.com/services.htm | `Hotel Equities/website/Hotel Equities Services.html` | Captured | No |
| P0 | `recy1oDTNe7kyQGbE` | Hotel Equities CALA | https://www.hotelequities.com/cala.htm | `Hotel Equities/website/Hotel Equities CALA.html` | Captured | No |

**Hard allowlist only** — no other Source Library rows for this operator.

**Note:** Live site uses `.htm` paths (`services.htm`, `cala.htm`), not extensionless `/services`.

---

## 2. Source Suitability for Extraction

| Source | Role (classifier) | Text available | Suitable? | Notes |
|--------|-------------------|----------------|-----------|-------|
| Home | `public_web` | ~3.8k chars (plain text) | **Yes** | Corporate overview, footprint stats, brand partners on page |
| Services | `public_web` | plain text (cheerio) | **Yes** | Management / development services |
| CALA | `public_web` | plain text (cheerio) | **Yes** | CALA division scope, leadership, pipeline — best match for `recWPKu5laVZxsvpn` |

### HTML normalization fix (2026-07-06)

`readLocalSourceText()` now routes `.html`/`.htm` through shared `parseHtmlDocument()` (cheerio): strips `script`, `style`, `nav`, `footer`, `noscript`, `svg`; extracts title + meta description + body text; collapses whitespace.

**Dual reference roots (2026-07-06):** Relative `Local File Path` values resolve against Brand Reference Material first, then Operator Reference Material. PDF enrichment uses `--source-group pdf` on the narrow extract script.

```bash
npm run hotel-equities-extract -- --dry-run --source-group pdf
```

Source groups: `website` (default), `pdf`, `all`. PDF IDs: `recxdPFckVzA3ckmN`, `rectqBTiGkq3hUlXa`.

**Preview before fix:**

| Metric | Value |
|--------|-------|
| Document size (home) | ~715k chars (raw HTML) |
| `op.snapshot.companyName` value | `<!DOCTYPE html>…` leakage |
| Text quality | Unusable for steward review |

**Preview after fix:**

| Metric | Value |
|--------|-------|
| Document size (home) | **3,846 chars** plain text |
| `op.snapshot.companyDescription` | Meta description sentence (no markup) |
| `op.snapshot.primaryServiceModel` | `third party management` with readable evidence |
| DOCTYPE/meta leakage | **None** |

Tests: `npm run test:partner-intelligence-extract-source-text`

**Still avoid** `runPartnerOperatorExtraction --apply` without narrow script — full registry + gap facts, not target-key-only.

---

## 3. Target Fact Keys

### Governance priority keys (stewardship plan)

| Requested key | In extraction registry? | Registry / proxy key |
|---------------|-------------------------|----------------------|
| `op.snapshot.companyName` | **Yes** | `op.snapshot.companyName` |
| `op.snapshot.companyDescription` | **Yes** | `op.snapshot.companyDescription` |
| `op.snapshot.parentCompany` | **Yes** | Only if page states parent — CALA page may say division of Hotel Equities |
| `op.platform.offeredServices` | **Yes** | `op.platform.offeredServices` |
| `op.capabilities.managementServices` | **No** | Use `op.platform.offeredServices` + `op.snapshot.primaryServiceModel` |
| `op.geography.regions` | **No** | Use `op.markets.regionsSupported` |
| `op.brandRelationships` | **No** | Use `op.brand.familiesOperated` if brands named on source |
| `op.ownerValueProposition` | **No** | Use engagement narrative proxies: `op.engagement.operatingCollaborationMode`, owner-facing copy from CALA page |
| `op.operatingModel` | **No** | Use `op.snapshot.primaryServiceModel` |

### Recommended first extract set (9 fields → 7 registry keys)

1. `op.snapshot.companyName` — expect **Hotel Equities** or **Hotel Equities (CALA)** by source
2. `op.snapshot.companyDescription`
3. `op.snapshot.parentCompany` — CALA source only if explicitly stated
4. `op.platform.offeredServices` — from services.htm
5. `op.snapshot.primaryServiceModel` — e.g. third-party management
6. `op.markets.regionsSupported` — CALA + home geography copy
7. `op.brand.familiesOperated` — only if Marriott/Hilton/Hyatt named with evidence

**Do not extract** full operator registry (~80+ fields) on first pass — `runPartnerOperatorExtraction` fills gaps with “Not confirmed” rows.

---

## 4. Existing Extraction Tooling

| Tool | Operator HTML support | Dry-run | Safe for HE today? |
|------|----------------------|---------|-------------------|
| `runPartnerSourceExtraction` | Per-source; uses `loadSourceDocumentText` | **No** | **No** — writes Pending facts + patches source |
| `runPartnerOperatorExtraction` | Batch; full registry + gap facts | **No** | **No** — may auto-approve extraction with `--force` |
| `partner-intelligence:extract-smoke` | N/A | Prints message only | **No** — not a real preview |
| `curio-clean-reextract.mjs` | Brand-only | **Yes** | N/A |
| `hotel-equities-extract-preview.mjs` | Read-only preview | **Yes** | **Yes** — no Airtable writes |

**LLM extraction:** `PARTNER_INTELLIGENCE_LLM_EXTRACTION_ENABLED` was **off** during preview; rules-only path used. Enabling LLM with cleaned text may improve quality after HTML fix.

---

## 5. Read-Only Preview (Dry-Run) — 2026-07-06

**Command run:**

```bash
node scripts/hotel-equities-extract-preview.mjs
```

**Reports:**

- `reports/hotel-equities-extraction-preview.json`
- `reports/hotel-equities-extraction-preview.md`

**Results:**

| Metric | Value |
|--------|-------|
| Sources previewed | 3 |
| LLM enabled | false |
| Registry keys missing (requested names) | 5 |
| Usable target-key candidates (rules, raw HTML) | **Poor quality** — HTML leakage |

**Conclusion:** Preview confirms sources are linked and produce **readable plain text**. Apply extraction should use the **narrow HE script** (planned) — not full operator batch extraction.

---

## 6. Recommended Extraction Command (Future)

### Phase A — Read-only preview (safe now)

```bash
npm run hotel-equities-extract -- --dry-run
```

Reports: `reports/hotel-equities-extract.{md,json}`

Optional flags: `--source-ids "rec…,rec…"`, `--fact-keys "op.snapshot.companyName,…"`, `--limit-facts 12`

Legacy preview (no filtering): `node scripts/hotel-equities-extract-preview.mjs`

### Phase B — Narrow apply (requires founder approval)

```bash
npm run hotel-equities-extract -- --apply --approve-hotel-equities-extract
```

### Do NOT use without narrow script

```bash
# BAD — full registry + gap facts even though HTML parse is fixed
node scripts/partner-intelligence-extract-smoke.mjs --apply --source=rectG9wdsAeL7u0FG
```

---

## 7. What Would Be Written on Apply (narrow script)

| Airtable object | Fields |
|-----------------|--------|
| **Extracted Facts** | New rows: `Human Review Status = Pending`, linked to operator + source, target field keys only |
| **Source Library** | Optional: `Status → Extracted`, `Extraction Run ID`, `Approved for Extraction = Yes` (human step first) |
| **Not written** | Operator Setup governance, Company Validated, External Display Status, Approved facts, Explorer-use approval |

---

## 8. What Should NOT Be Extracted

- Full operator registry gap-fill (“Not confirmed in available sources” for 70+ fields)
- Facts from non-allowlisted sources
- `op.snapshot.parentCompany` unless explicitly stated (do not infer Hilton/Marriott as parent)
- Brand relationship claims without named flags on the same page
- Third-party press / OTA / blog content
- Explorer fixture JSON (`fixtures/operator-*-explorer-he-cala.json`) — not PI evidence
- Vague marketing superlatives without evidence quotes
- Raw HTML blobs as field values

---

## 9. Manual Review Checklist (Post-Extraction)

- [ ] Each fact value is **plain language**, not HTML markup
- [ ] Evidence text quotes a **readable sentence** from the correct source URL
- [ ] `op.snapshot.companyName` matches Hotel Equities / HE CALA naming on source
- [ ] CALA geography facts trace to **CALA page** or explicit CALA copy on home
- [ ] Brand flags only approved if **named** on official page (Marriott, Hilton, Hyatt families)
- [ ] Reject duplicate field keys across sources unless edited merge
- [ ] Reject inferred parent company if not stated
- [ ] **Do not** set Human Review Status = Approved during extraction
- [ ] **Do not** set Approved for Explorer Use on sources until steward review
- [ ] **Do not** run profile governance publish until readiness dry-run clean

---

## 10. Next Steps After Extraction

1. **Steward review** — approve 3–8 facts only (`npm run steward-partner-intelligence -- --dry-run --recompute`)
2. **Source steward** (after review) — advance status + Explorer use on allowlisted sources only
3. **Readiness audit** — `npm run audit-partner-intelligence-publish-readiness`
4. **Governance publish dry-run** — `npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run`
5. **Never** Company Validated from PI path

---

## Implementation Dependencies

| Dependency | Owner | Blocks |
|------------|-------|--------|
| HTML text normalization in `extract-source-text.js` for local `.html` | **Done** (2026-07-06) | Quality extraction from captured files |
| `hotel-equities-extract.mjs` narrow apply script | **Done** (2026-07-06) | Safe controlled apply |
| Optional: enable LLM extraction with cleaned text | Steward + env | Higher-quality evidence quotes |
| Human source review | Steward | Approved for Extraction / Explorer Use |

---

## Related Reports

| Report | Path |
|--------|------|
| Extraction preview (read-only) | `reports/hotel-equities-extraction-preview.{md,json}` |
| Stewardship package | `reports/partner-intelligence-stewardship-package.md` |
| Source capture | [hotel-equities-source-capture-plan.md](./hotel-equities-source-capture-plan.md) |

**Last updated:** 2026-07-06
