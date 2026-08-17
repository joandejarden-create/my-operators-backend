# Hotel Equities — Partner Intelligence Production Plan

**Date:** 2026-07-06  
**Status:** Discovery complete — **source capture required before PI stewardship**  
**Target:** Hotel Equities (CALA) — Operator Setup - Master  
**Record ID:** `recWPKu5laVZxsvpn`

> **Authority:** [partner-intelligence-priority-profile-production-tracker.md](./partner-intelligence-priority-profile-production-tracker.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md)

---

## Executive Summary

| Question | Answer |
|----------|--------|
| **Operator Setup record exists?** | **Yes** — `recWPKu5laVZxsvpn` · **Hotel Equities (CALA)** |
| **Separate parent “Hotel Equities” Master row?** | **Not found** in live Operator Master scan (2026-07-06). Only the CALA division record exists. |
| **PI sources linked?** | **No** — 0 Source Library rows |
| **Extracted facts?** | **No** — 0 Extracted Facts rows |
| **Approved Explorer-use sources?** | **0** |
| **Approved/Edited facts?** | **0** |
| **Stewardship dry-run runnable?** | **Yes** (ran 2026-07-06) — empty package; no recommendations |
| **Profile governance publish ready?** | **No** — blocked at source capture |
| **Next step** | [hotel-equities-extraction-plan.md](./hotel-equities-extraction-plan.md) — extraction preview complete; build narrow apply script after HTML parse fix |

---

## 1. Operator Setup Record

| Field | Value |
|-------|-------|
| **Record ID** | `recWPKu5laVZxsvpn` |
| **Table** | Operator Setup - Master |
| **Display name** | Hotel Equities (CALA) |
| **Explorer** | `/operator-explorer-gold-mock.html?id=recWPKu5laVZxsvpn` |
| **Operator Setup** | `/third-party-operator-setup-new-two.html?recordId=recWPKu5laVZxsvpn` |

**Naming note:** The priority tracker lists **Hotel Equities** as the next operator profile. In Airtable today, the linked Master record is the **CALA regional division** (`Hotel Equities (CALA)`), not a separate enterprise-wide operator row. Treat this record as the PI package target unless a parent Master row is created later.

### Current profile governance (Setup root — not PI)

Read-only snapshot (2026-07-06):

| Field | Live value |
|-------|------------|
| Validation Status | — |
| Usage Permission | — |
| Source Type | Imported sample data |
| Data Confidence Level | Public-source |
| Last Reviewed Date | — |
| Company Validated | false |
| External Display Status | — |

Explorer child tables and fixtures (`fixtures/operator-*-explorer-he-cala.json`) contain **research-based CALA copy** — useful for Explorer hydration but **not** PI evidence until captured as Source Library rows and reviewed facts.

---

## 2. Partner Intelligence Package Status

### Sources

| Metric | Count |
|--------|-------|
| Source Library rows linked to `recWPKu5laVZxsvpn` | **0** |
| Sources with title match “Hotel Equities” (unlinked) | **0** |
| Approved for Explorer Use = Yes | **0** |

### Facts

| Metric | Count |
|--------|-------|
| Extracted Facts linked to operator | **0** |
| Human Review Status = Approved | **0** |
| Human Review Status = Edited | **0** |
| Pending / Rejected | **0** |

### Publish readiness audit

Hotel Equities does **not** appear in `reports/partner-intelligence-publish-readiness.{md,json}` (2026-07-06) because no PI package is assembled for this operator.

---

## 3. Stewardship Dry-Run (2026-07-06)

**Command run (dry-run only, no writes):**

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

**Report:** `reports/partner-intelligence-stewardship-package.md`

| Result | Value |
|--------|-------|
| Sources in package | 0 |
| Facts in package | 0 |
| Current eligible | **false** |
| Blockers | `no_linked_sources`; `no_approved_explorer_sources`; `no_approved_facts` |
| Recommended facts | none (no sources to score) |

**Do not run `--apply`** until sources exist and steward has reviewed explicit `--approve-source-ids` / `--approve-fact-ids`.

---

## 4. Current Blockers

| Blocker | Severity | Fix |
|---------|----------|-----|
| `no_linked_sources` | **Hard** | Register PI Source Library rows linked to `recWPKu5laVZxsvpn` |
| `no_approved_explorer_sources` | **Hard** | Human review → **Approved for Explorer Use = Yes** on clean sources |
| `no_approved_facts` | **Hard** | Run extraction → approve/edit 3–8 governance-priority facts |
| No operator harvest profile | **Planning** | `operator-reference-registry.js` has no Hotel Equities entry yet (unlike Arbor) |
| No local operator reference folder | **Planning** | `data/operator-sources/` not present; use `partner-reference:init-folder` |
| Enterprise vs CALA scope | **Decision** | Confirm whether PI package is CALA-only or needs parent Hotel Equities Master row |

---

## 5. Recommended Source Records to Review

Capture from **official Hotel Equities materials** first. URLs from `scripts/brand-reference-material-companies.json` (`hotel_equities`) and live Operator Setup:

| Priority | URL / material | Type | Region | Notes |
|----------|----------------|------|--------|-------|
| P0 | https://www.hotelequities.com/ | Company website | Americas | Corporate positioning, services overview |
| P0 | https://www.hotelequities.com/services | Management services | Americas | Operating model, owner-facing services |
| P0 | https://hotelequities.com/cala.htm | CALA division page | CALA | Regional scope, leadership, pipeline — aligns with Master record name |
| P1 | Hotel Equities press / news (official) | Press release | Americas | Supporting only; do not lead governance rollup |
| P2 | Saved CALA public positioning (fixtures cite HE CALA 2025 launch) | Internal reference | CALA | Cross-check against web capture; not a substitute for Source Library |

**Do not use** third-party blogs, OTA pages, or census operator strings as primary governance evidence.

### Suggested capture workflow (Arbor pattern)

1. Initialize reference folder (human apply when ready):

```bash
npm run partner-reference:init-folder -- --company "Hotel Equities" --dry-run
```

2. Download / capture official pages (example — **dry-run first**):

```bash
npm run partner-reference:download -- --url "https://www.hotelequities.com/services" --company "Hotel Equities" --type operator-capability-deck --title "Hotel Equities Services" --dry-run
npm run partner-reference:download -- --url "https://hotelequities.com/cala.htm" --company "Hotel Equities" --type operator-capability-deck --title "Hotel Equities CALA" --dry-run
```

3. Register rows in **Partner Intelligence - Source Library** with:
   - Profile Type = **Operator**
   - Operator link = **`recWPKu5laVZxsvpn`**
   - Status = **Found** or **Captured** → advance after review
   - Source Origin = **Operator Provided** (company site) or **Public Web** (if strictly public pages)
   - Region = **CALA** for division materials; **Americas** for corporate pages

4. Run extraction after **Approved for Extraction** — then human fact review.

**Future improvement (out of scope here):** Add `hotel_equities` profile to `lib/partner-intelligence/operator-reference-registry.js` and optional `seed-hotel-equities-pilot-sources.mjs` mirroring Arbor.

---

## 6. Recommended Fact Areas to Capture / Review

Map to `OPERATOR_GOVERNANCE_FIELD_KEYS` in `lib/partner-intelligence/stewardship-package.js`:

| Area | Registry field keys | Steward priority |
|------|---------------------|------------------|
| Company name | `op.snapshot.companyName` | P0 |
| Parent / ownership | `op.snapshot.parentCompany` | P1 (if stated on official site) |
| Summary | `op.snapshot.summary`, `op.snapshot.companyDescription` | P0 |
| Management services | `op.capabilities.managementServices` | P0 |
| Operating model | `op.operatingModel` | P0 |
| Geography / regions | `op.geography.regions` | P0 — emphasize **CALA** for this Master row |
| Brand relationships | `op.brandRelationships` | P1 — Marriott, Hilton, Hyatt families cited in Explorer copy |
| Owner value proposition | `op.ownerValueProposition` | P0 |

**Target for first publish scope:** ≥3 approved facts including identity/summary **plus** at least one capabilities/geography/operating-model fact (required for **High** confidence; **Medium** acceptable for sparse batches).

---

## 7. Can Stewardship Dry-Run Run Now?

**Yes — already ran.** It completes with an **empty package** and blockers above. Re-run after each source-capture milestone:

```bash
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

---

## 8. Exact Next Command

**Source capture is the gating step.** Run reference folder init in **dry-run** first:

```bash
npm run partner-reference:init-folder -- --company "Hotel Equities" --dry-run
```

After founder approves folder creation and downloads, register Source Library rows (manual Airtable or future seed script), then:

```bash
npm run audit-partner-intelligence-publish-readiness
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

---

## 9. External Web / Source Capture Needed?

**Yes — required.**

- No PI sources or facts exist today.
- No Hotel Equities entry in `operator-reference-registry.js` for `partner-reference:harvest-operators`.
- Official URLs are catalogued in `scripts/brand-reference-material-companies.json` but not yet harvested or registered in Source Library.

---

## 10. Recommended Profile Governance Outcome (if clean)

Assuming reviewed **company website / CALA division** sources with directly stated facts:

| Field | Likely proposal |
|-------|-----------------|
| Validation Status | **Source-Informed** (public/company web) or **Company Published** if operator-provided PDFs/decks reviewed |
| External chip (`displayLabel`) | **Source-Informed Profile** (public web) or **AI-Assisted Profile** (company materials mapped conservatively) |
| Confidence Level | **Medium** until ≥3 substantive approved facts; avoid **High** on identity-only sparse scope |
| Source Region | **CALA-Specific** (for this Master row) |
| Company Validated | **Never** from PI alone — requires direct Hotel Equities attestation |

**Never set:** Company Validated, Company Validation Date.

---

## 11. Standard Command Sequence (after sources exist)

```bash
# 1. Package audit
npm run audit-partner-intelligence-publish-readiness

# 2. Stewardship (dry-run)
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute

# 3. After manual source/fact approval in Airtable — re-audit until eligible

# 4. Governance publish dry-run
npm run publish-partner-intelligence-profile-governance -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run

# 5. Apply only after founder approval
# npm run publish-partner-intelligence-profile-governance -- --apply --entity-type operator --target-rec-id recWPKu5laVZxsvpn
```

---

## 12. Lessons from Completed Pilots (apply to Hotel Equities)

| Pilot | Apply to HE |
|-------|-------------|
| **Arbor** | Seed 3–7 linked sources before stewardship; CALA regional sources; public web → Source-Informed chip |
| **Kimpton** | Approve Explorer-use sources narrowly; 4 substantive facts → High confidence |
| **Curio** | Do not bulk-approve; validate identity values; exclude noisy sources from publish scope |

Hotel Equities should follow the **Arbor operator path** (clean public/company sources) rather than the Curio recovery path.

---

## Related Artifacts

| Artifact | Path |
|----------|------|
| **Source capture plan** | [hotel-equities-source-capture-plan.md](./hotel-equities-source-capture-plan.md) |
| Stewardship dry-run report | `reports/partner-intelligence-stewardship-package.md` |
| Priority tracker | `docs/data-intelligence/partner-intelligence-priority-profile-production-tracker.md` |
| Operator Explorer fixtures | `fixtures/operator-*-explorer-he-cala.json` |
| Reference URL catalog | `scripts/brand-reference-material-companies.json` (`hotel_equities`) |
| Collection guide | `docs/partner-reference-material-collection-guide.md` |

**Last updated:** 2026-07-06 — read-only Airtable discovery + stewardship dry-run.
