# Partner Intelligence — Priority Profile Production Tracker

**Date:** 2026-07-06  
**Status:** Active operating tracker — small-batch Brand + Operator Explorer profile production  
**Scope:** Documentation and operations only. Update this file as packages move through the PI → Profile Governance pipeline.

> **Authority:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md), [INTELLIGENCE_GOVERNANCE.md](./INTELLIGENCE_GOVERNANCE.md)

---

## 1. Purpose

Track the **next batch** of Brand and Operator Explorer profiles moving through the proven Partner Intelligence → Profile Governance pipeline at **small-batch scale** (one profile at a time, dry-run first, explicit founder approval before apply).

**Pipeline proven on (2026-07-06):**

| Path | Profile | Lesson |
|------|---------|--------|
| **Operator (clean)** | Arbor Lodging (CALA) | Stewardship + publish on approved public/company sources; sparse fact cap applies |
| **Brand (clean)** | Kimpton Hotels | Multi-source company-materials package; High confidence when ≥3 substantive facts |
| **Brand (recovery)** | Curio Collection by Hilton | Package integrity cleanup, narrow re-extract, publish-scope eligibility, sparse Medium confidence |
| **Operator (production)** | Hotel Equities (CALA) | Website + PDF capture, narrow extract, manual PDF curation when noisy |
| **Operator (from scratch)** | GHL Hoteles (GHL Holding) | Official EN source capture → narrow extract → governance publish → stable **no_op** |

**Next system milestone (2026-07-06):** [Approved Intelligence → Platform Field Publishing v1](./approved-intelligence-platform-field-publishing-v1.md) — `npm run approved-intelligence-field-publishing-audit` maps approved facts to product destinations (read-only).

**Batch operations:** [Dealality Intelligence Production Workflow v1.1](./dealality-intelligence-production-workflow-v1.md#11-implementation-v11--batch-queue) — `npm run intelligence-production-queue -- --plan`.

**Legacy active brands (governance upgrade only):** [Active Brand Profile Governance Upgrade v1](./active-brand-governance-upgrade-v1.md) — `npm run active-brand-governance-upgrade -- --dry-run` audits 11 Explorer-active Choice/Hilton brands without rebuilding Explorer content.

**Choice legacy source packages:** [Choice Legacy Brand Source Package v1](./choice-legacy-brand-source-package-v1.md) — `npm run choice-legacy-brand-source-package -- --dry-run` plans P0/P1 PI sources for 8 Choice brands missing evidence.

**Choice mini-batch 1:** [choice-legacy-brand-mini-batch-1.md](./choice-legacy-brand-mini-batch-1.md) — `npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-1 --dry-run` (Comfort, Everhome, Quality — **Platform Ready**).

**Choice mini-batch 2:** **Platform Ready** — Country Inn, Radisson, Radisson Individuals, Radisson RED. Orchestrator: `npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --dry-run`.

**This tracker does not replace Airtable.** Record IDs marked **TBD** must be resolved in Brand Setup - Brand Basics or Operator Setup - Master before running targeted commands.

---

## 2. Standard Workflow

Per profile, run in order. Default is **dry-run / report-only** until human approval.

```
1. Inventory PI package (sources + facts linked to Setup root rec…)
2. npm run audit-partner-intelligence-publish-readiness
3. Review publish-scope blockers vs full-package diagnostics
4. npm run steward-partner-intelligence -- --entity-type … --target-rec-id … --dry-run
5. Manual Airtable review: approve Explorer-use sources + approve/edit facts (narrow subset)
6. Optional targeted re-extract (package-specific scripts only when documented)
7. Re-run audit until publish-scope eligible (or document blockers)
8. npm run publish-partner-intelligence-profile-governance -- --entity-type … --target-rec-id … --dry-run
9. Founder review: proposed governance + expected Explorer chip
10. npm run publish-partner-intelligence-profile-governance -- --apply …  (explicit approval only)
11. Post-apply audit → expect change class no_op
12. UI QA: Explorer trust chip + API governance object
```

**Publish scope rule:** Eligibility uses only sources with **Approved for Explorer Use = Yes** and **Approved/Edited** facts linked to those sources. Non-approved linked sources are diagnostics only.

**Sparse confidence cap:** Proposed **Confidence Level** capped at **Medium** when publish scope has &lt;3 approved facts and/or identity-only coverage. **High** requires identity + substantive field coverage.

**Never from PI publish alone:** Company Validated, Company Validation Date.

---

## 3. Status Columns

| Column | Values / meaning |
|--------|------------------|
| **Entity Name** | Brand or operator display name |
| **Entity Type** | `brand` \| `operator` |
| **Record ID** | Brand Basics or Operator Master `rec…` |
| **Priority** | `completed` \| `next` \| `queued` \| `blocked` \| `defer` |
| **PI Source Status** | `none` \| `linked` \| `extracted` \| `partially approved` \| `stewarded` |
| **Approved Sources** | Count in **publish scope** (Explorer Use = Yes) |
| **Approved Facts** | Count in **publish scope** (Approved/Edited, linked to approved sources) |
| **Readiness Status** | `not audited` \| `eligible` \| `blocked` \| `no_op` |
| **Governance Publish Status** | `not started` \| `dry-run ok` \| `applied` \| `no_op` |
| **External Label** | Expected `governance.displayLabel` from normalizer |
| **Last Reviewed Date** | From Setup or PI proposal |
| **Open Issues** | Blockers, sparse warnings, integrity flags |
| **Next Action** | Single concrete steward step |

---

## 4. Suggested First 10 Profiles

| Entity Name | Entity Type | Record ID | Priority | PI Source Status | Approved Sources | Approved Facts | Readiness Status | Governance Publish Status | External Label | Last Reviewed Date | Open Issues | Next Action |
|-------------|-------------|-----------|----------|------------------|------------------|----------------|------------------|---------------------------|----------------|-------------------|-------------|---------------|
| Arbor Lodging (CALA) | operator | `recF5Z87OAqFgndoq` | **completed** | stewarded | 7 | 1 | **no_op** | **applied** | Source-Informed Profile | 2026-06-10 | Sparse fact set (1 approved fact); confidence Medium | Monitor; enrich facts before expecting High |
| Kimpton Hotels | brand | `recCKuXCmGvxHPfb3` | **completed** | stewarded | 4 | 4 | **downgrade** (protected) | **applied** | AI-Assisted Profile · Company Materials | 2026-06-12 | Live **Company Published** stronger than stale PI proposal (`Source-Informed` / `Unknown`); queue Stage 8 protected | Monitor; do not re-apply governance (downgrade blocked) |
| Curio Collection by Hilton | brand | `receQkxgjlezsc1xg` | **completed / sparse** | stewarded (narrow) | 1 | 2 | **no_op** | **applied** | AI-Assisted Profile | 2026-07-06 | 14 excluded sources; identity-only; 24 rejected facts; Internal Notes label `operator:` on live row (code fixed) | Enrich from fact sheet source; fix Internal Notes on re-apply or manual edit |
| Hotel Equities (CALA) | operator | `recWPKu5laVZxsvpn` | **completed** | stewarded (3) + PDF registered (2) | 3 | 5 | **no_op** | **applied** | AI-Assisted Profile | 2026-07-06 | PDF extract dry-run: manual curation — [pdf-manual-curation-plan](./hotel-equities-pdf-manual-curation-plan.md) | Manual 2–4 Pending facts → steward PDF sources selectively |
| GHL Hotels | operator | `reciI2tYQBfMoMK9G` | **completed** | stewarded (5 EN) | 5 | 5 | **no_op** | **applied** | AI-Assisted Profile | 2026-07-06 | ES home excluded; 2 facts left Pending — [ghl-hotels-pi-production-plan](./ghl-hotels-pi-production-plan.md) | **Stage 8** — monitor via `intelligence-production-queue` |
| Aimbridge Hospitality (LATAM/CALA) | operator | **TBD** | **next** | not started | — | — | not audited | not started | — | — | **No Operator Setup - Master row found** (2026-07-06 Airtable search; 500-row scan + PI Source Library empty) | Create Operator Master record when ready; then inventory PI sources |
| Best Western Plus | brand | `rec5KPgalPPAFl7UZ` | **next** | partial | — | — | not audited | pilot only | AI-Assisted Profile (pilot) | — | Governance pilot applied via `pilot-profile-governance-values`; **not** full PI pipeline | Run PI readiness audit; link PI sources if missing |
| Hilton Garden Inn | brand | `recrvdAjRlXxPvPPF` | **next** | not started | — | — | not audited | not started | — | — | Parent: Hilton Worldwide; single canonical Brand Basics row | Run PI readiness audit; link Hilton company-materials sources |
| Radisson Blu (Choice) | brand | `recWPEvxBQxVVzSq3` | **completed** | stewarded (4) | 4 | 8 | **no_op** | **applied** | AI-Assisted Profile · Company Materials | 2026-07-06 | 3 facts Pending; governance published — [radisson-blu-pi-production-plan](./radisson-blu-pi-production-plan.md) | **Stage 8** — monitor via `active-brand-governance-upgrade` |
| Viento Sur Gestión Hotelera (CALA) | operator | `recZPHT2zqc8K6itx` | **next** | not started | — | — | not audited | pilot only | Source-Informed Profile (pilot) | — | Census/CALA priority operator; governance pilot only | Run full PI pipeline after source capture |

**Record ID resolution:** Search Brand Setup - Brand Basics / Operator Setup - Master in Airtable, or use Explorer URL `?id=rec…`. Update this table when IDs are confirmed.

---

## 5. Commands to Run Per Profile

Replace `{entityType}`, `{recId}` with `brand`/`operator` and the Setup root record ID.

### A. Package-wide audit (all profiles)

```bash
npm run audit-partner-intelligence-publish-readiness
```

Reports: `reports/partner-intelligence-publish-readiness.{md,json}`

### A2. Batch queue (preferred for priority tracker)

```bash
npm run intelligence-production-queue -- --plan
```

Optional filters: `--entity-type operator`, `--entity-type brand`, `--stage 8`, `--ready-only`, `--blocked-only`

Reports: `reports/intelligence-production-queue.{md,json}`

Per-entity detail: `npm run intelligence-profile-workflow -- --entity-type … --target-rec-id rec… --plan`

### B. Targeted stewardship (dry-run default)

```bash
npm run steward-partner-intelligence -- --entity-type {entityType} --target-rec-id {recId} --dry-run
```

Optional: `--recompute` (ignore cached readiness JSON), `--approve-source-ids rec…,rec…` and `--approve-fact-ids rec…` **only after manual review** for apply.

Reports: `reports/partner-intelligence-stewardship-package.{md,json}`

### C. Profile governance publish (dry-run → apply)

```bash
npm run publish-partner-intelligence-profile-governance -- --entity-type {entityType} --target-rec-id {recId} --dry-run
```

After founder approval:

```bash
npm run publish-partner-intelligence-profile-governance -- --apply --entity-type {entityType} --target-rec-id {recId}
```

Reports: `reports/partner-intelligence-profile-governance-publish.{md,json}`

### D. Post-apply verification

```bash
npm run audit-partner-intelligence-publish-readiness
npm run test:profile-governance-normalizer
```

### E. Package-specific (use only when documented)

| Package | Command | When |
|---------|---------|------|
| Curio | `npm run curio-clean-reextract -- --dry-run` | Wrong-brand / narrow re-extract path only |
| Curio | `npm run quarantine-curio-pi-facts -- --dry-run` | Contaminated fact quarantine |
| Arbor (legacy) | `npm run steward-arbor-pi-pilot -- --dry-run` | Reference only — prefer generalized stewardship |

### F. Reference discovery (pre-PI)

```bash
npm run partner-reference:search -- --operator "Hotel Equities" --domain hotelequities.com
```

### G. Active legacy brands — governance upgrade audit (read-only)

For Explorer-active brands built before PI governance (do **not** rebuild Explorer content):

```bash
npm run active-brand-governance-upgrade -- --dry-run
```

Reports: `reports/active-brand-governance-upgrade.{md,json}` · See [active-brand-governance-upgrade-v1.md](./active-brand-governance-upgrade-v1.md)

**v1 batch (11):** Ascend, Comfort Inn & Suites, Country Inn & Suites by Choice, Curio, Everhome, Kimpton, Quality Inn, Radisson Blu, Radisson, Radisson Individuals, Radisson RED.

**Choice source package (8 legacy):** `npm run choice-legacy-brand-source-package -- --dry-run` — see [choice-legacy-brand-source-package-v1.md](./choice-legacy-brand-source-package-v1.md)

**Mini-batch 1 (Comfort + Everhome + Quality):** `npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-1 --dry-run` — [choice-legacy-brand-mini-batch-1.md](./choice-legacy-brand-mini-batch-1.md) — **Platform Ready**.

**Mini-batch 2 (Country Inn + Radisson family):** **Platform Ready** — `npm run choice-legacy-batch-pipeline -- --batch mini-batch-2 --dry-run`

**Ascend (Choice legacy mini-batch 3):** **Platform Ready** (2026-07-07) — `brochure--ascend.pdf` + consumer + press; 3 PI sources, 5 approved facts; orchestrator: `npm run choice-legacy-batch-pipeline -- --batch mini-batch-3 --dry-run`

**Source auto-resolution (reusable):** `lib/partner-intelligence/brand-source-auto-resolver.js` — resolves approved/linked/company-controlled Source Library rows by brand record id, classifies role, and generates the extraction allowlist. Removes the manual `allowlistedSourceIds` config edit. Choice pipeline prefers a non-empty manifest allowlist and auto-resolves only when empty (mini-batch 1/2/3 unchanged; validated against Ascend live sources).

**Tribute Portfolio by Marriott (full package pilot):** **Text/governance Platform Ready** (2026-07-07) — brand `recCvV0PuZOi8c3hC`. Stages 1–4 complete: package dry-run, apply plan, one-command pipeline apply (6 sources, 7 approved facts, governed Platform Ready, AI-Assisted Profile / Company Materials). Stage 4 targeted extraction fixed generic gap placeholders. **Stage 5 asset/PR governance (read-only v1):** `npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run` — logo/hero/image/PR gap audit; official Marriott candidates; Rendered Source Capture v1 needed for newsroom. **Stage 6 asset registry workflow (staging v1):** `npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run` → `reports/brand-asset-registry-workflow.{md,json}` — proposes Brand Asset Registry schema, stages Tribute approval plan (logo/hero/gallery/PR placeholder). Schema apply gated `--apply --approve-brand-asset-registry-schema`. No image downloads or Brand Setup overwrites. See [brand-asset-registry-workflow-v1.md](./brand-asset-registry-workflow-v1.md).

**Tribute Portfolio — Stage 7 visual slot requirements (v1):** `npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --dry-run` → `reports/brand-explorer-visual-slot-requirements.{md,json}` — defines slot-specific image requirements (Logo, Hero, Gallery, Recent Openings, Value Driver, Brand Standards, PR link) and audits the 9 registry records. Findings: logo candidates need usage review; hero + 3 gallery images need named-property/CALA confirmation; FDD is source-reference only; Mock/Demo hero + newsroom PR remain guarded; **Recent Openings** and **Where This Brand Creates the Most Value** slots remain **Missing**. Also demotes any registry record improperly marked "Approved For Explorer Use" (recommendation only). Schema apply gated `--apply --approve-brand-visual-slot-schema`; status corrections report-only. No image downloads, no Brand Setup media writes.

**Tribute Portfolio — Stage 8 CALA property visual discovery (v1/v2):** `npm run cala-tribute-property-visual-discovery -- --dry-run` → `reports/cala-tribute-property-visual-discovery.{md,json}` — crawls CALA Marriott sitemaps + seeds for named Tribute Portfolio hotels; extracts metadata-only image URL candidates from official property `/photos/` pages (v2 cover resolution; Puppeteer does not bypass Akamai on `/overview/`). Registry apply gated `--apply --approve-cala-tribute-visual-candidates`. 27 registry records after v2 apply (9 original + 12 v1 CALA + 6 v2 gallery). No image downloads, no Brand Setup writes.

**Tribute Portfolio — Stage 9 visual slot review & candidate selection (v3):** `npm run tribute-visual-asset-slot-review -- --dry-run` → `reports/tribute-visual-asset-slot-review.{md,json}` — groups all Tribute registry records by Explorer slot; identifies competing v1/v2 candidates; scores quality; recommends primary + alternates for human usage review; marks weak v1 generic crops as superseded (not deleted). Selection apply gated `--apply --approve-tribute-visual-slot-selection`. Applied 2026-07-07: 11 primaries, 8 alternates, 5 superseded. Never approves Explorer use, never downloads images, never writes Brand Setup media.

**Tribute Portfolio — Stage 10 human review readiness (v4):** `npm run brand-asset-human-review-readiness -- --brand tribute-portfolio --dry-run` → `reports/brand-asset-human-review-readiness.{md,json}` — evaluates whether each of the 11 primary candidates has enough metadata/source context for human usage review; produces per-candidate checklist. Report-only; no Airtable writes.

**Tribute Portfolio — Stage 11 review decision writer (v5/v5.1):** `npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --dry-run` → `reports/brand-asset-review-decision-writer.{md,json}` — applies explicit human approval/rejection to selected registry record IDs and audits approval-state consistency. Apply gated `--apply --approve-brand-asset-review-decisions` or `--apply --approve-brand-asset-approval-state-corrections`. Never auto-approves; never downloads; never writes Brand Setup media.

**Tribute Portfolio — Stage 12 download/attachment writer (v6):** `npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --dry-run` → `reports/brand-asset-download-attachment-writer.{md,json}` — selects only formally approved records, validates approved Source URLs (HTTP/content-type/size), plans deterministic local staging under `data/partner-intelligence/assets/tribute-portfolio/`, and (apply-gated) writes only Brand Asset Registry `Attachment` + `Local File Path` + staging notes. Apply gated `--apply --approve-brand-asset-download-attachments`. Never writes Brand Setup/Explorer media fields and never changes approval statuses.

**Tribute Portfolio — Stage 13 Explorer media promotion writer (v7):** `npm run explorer-media-promotion-writer -- --brand tribute-portfolio --dry-run` → `reports/explorer-media-promotion-writer.{md,json}` — inspects live Brand Setup + Brand Explorer Presentation media fields, filters only formally approved/attachment-ready registry assets, and proposes governed logo/hero/gallery/value-driver promotions with explicit overwrite blockers. Apply gated `--apply --approve-explorer-media-promotion` with optional overwrite flags `--allow-logo-overwrite`, `--allow-nonblank-hero-overwrite`, `--allow-presentation-slot-overwrite`. Never promotes Recent Openings/PR/FDD, never writes Company Validated fields, and never changes non-media narrative/scoring/schema fields.

**Tribute Portfolio — Stage 14 presentation copy promotion writer (v9):** `npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --dry-run` → `reports/brand-explorer-presentation-copy-promotion-writer.{md,json}` — reads v8.5 parity recommendations and proposes copy-only `Title`/`Body` updates on existing promoted presentation slots (`overview.hero`, gallery 1/2/4/5/6, scenario 1/2). Apply gated `--apply --approve-brand-explorer-copy-promotion` with optional `--allow-nonblank-copy-overwrite`. Never writes images, Brand Setup fields, registry records, or Company Validated fields.

**Tribute Portfolio — Stage 15 full Brand Explorer content parity audit (v10):** `npm run tribute-brand-explorer-content-parity-audit -- --dry-run` → `reports/tribute-brand-explorer-content-parity-audit.{md,json}` — full section/field parity audit versus completed brands (Kimpton, Curio, Radisson Blu/Choice, Radisson/Choice, Ascend) with staged content plan for v11. Read-only; no Airtable writes, no image/media changes, no Brand Setup overwrites.

**Batch pipeline orchestrator:** [choice-legacy-batch-pipeline-v1.md](./choice-legacy-batch-pipeline-v1.md) — one command replaces 7+ stage commands per batch.

**Mini-batch 1 stewardship:** `npm run choice-legacy-batch-source-stewardship -- --dry-run` — [choice-legacy-batch-source-stewardship-v1.md](./choice-legacy-batch-source-stewardship-v1.md)

**Mini-batch 1 URL capture:** `npm run choice-legacy-batch-url-capture -- --dry-run` — [choice-legacy-batch-url-capture-v1.md](./choice-legacy-batch-url-capture-v1.md)

**Mini-batch 1 extraction:** `npm run choice-legacy-batch-extract -- --dry-run` — [choice-legacy-batch-extraction-v1.md](./choice-legacy-batch-extraction-v1.md)

**Mini-batch 1 fact stewardship:** `npm run choice-legacy-batch-fact-stewardship -- --dry-run` — [choice-legacy-batch-fact-stewardship-v1.md](./choice-legacy-batch-fact-stewardship-v1.md)

**Mini-batch 1 governance publish:** `npm run choice-legacy-batch-governance-publish -- --dry-run` — [choice-legacy-batch-governance-publish-v1.md](./choice-legacy-batch-governance-publish-v1.md)

**Mini-batch 1 status (2026-07-07):** Comfort (`recOzH5iAE1xEjyD0`), Everhome (`recqkkrsevi4r9ibj`), Quality (`recd8o4k1JddhkRWW`) — governance **applied**; readiness equivalence `equivalent_stable_live_governance`; **Platform Ready** in `active-brand-governance-upgrade` when live chip is AI-Assisted Profile · Company Materials.

---

## 6. Review Checklist

Before stewardship apply or governance publish dry-run:

- [ ] Entity link resolved — PI sources/facts point to correct Brand Basics or Operator Master `rec…`
- [ ] Publish-scope sources reviewed — only approve **Approved for Explorer Use = Yes** on clean, on-brand sources
- [ ] Facts reviewed — **Approved** or **Edited** only; Rejected/quarantined facts excluded from recommendations
- [ ] Identity values match target brand/operator (no cross-brand leakage — see Curio lesson)
- [ ] Origin conflict checked **within publish scope** (not blocked by excluded full-package sources)
- [ ] `sparse_publish_scope_fact_set` warning understood if &lt;3 facts
- [ ] Proposed **Confidence Level** acceptable (Medium cap for sparse/identity-only)
- [ ] **Company Validated** remains false unless separate company attestation
- [ ] Expected external chip (`displayLabel` / `displaySubtitle`) is conservative and honest
- [ ] Protection rules clear — no HOLD markers, no Company Validated block

---

## 7. Apply Approval Checklist

Founder or designated steward must explicitly approve before `--apply`:

- [ ] Readiness audit shows **eligible** on **publish scope**
- [ ] Publish dry-run field diff reviewed (`reports/partner-intelligence-profile-governance-publish.md`)
- [ ] Evidence Notes accurately describe approved sources and fact count
- [ ] Internal Notes will use correct entity label (`brand:rec…` or `operator:rec…`) on next apply
- [ ] No downgrade of existing validation/confidence blocked by protection rules
- [ ] Sparse/identity-only package accepted at **Medium** confidence (not High)
- [ ] Full linked package exclusions documented (noisy sources not used for governance)
- [ ] Rollback plan: revert Setup governance fields manually if chip is wrong

---

## 8. UI QA Checklist

After apply:

- [ ] Brand Explorer or Operator Explorer loads without errors
- [ ] Trust chip visible when **External Display Status = Show Trust Label**
- [ ] `governance.displayLabel` matches audit expectation (e.g. Source-Informed Profile, AI-Assisted Profile)
- [ ] `governance.displaySubtitle` includes Last Reviewed, Source Basis, Region — **no confidence in subtitle**
- [ ] API check: `/api/brand-library/brand?brandId={recId}` or operator equivalent returns `governance` object
- [ ] Chip does not show Company Validated / owner-attested language unless Company Validated is true
- [ ] Re-run readiness audit — **change class `no_op`**

---

## 9. Known Blocked / Orphan Cleanup Queue

Items that are **not** the main publish path but should stay visible for stewards.

| Item | Type | Record ID | Issue | Recommended fix |
|------|------|-----------|-------|-----------------|
| Curio Collection Fact Sheet May 2026 (orphan package) | source/package | `recA7VOMWhmTtiklj` | Missing entity link; no approved Explorer sources | Link to `receQkxgjlezsc1xg` or merge after dedupe with `recL1qfHCOAUZr9Rz` |
| 2025 Mexico Curio FDD (orphan package) | source/package | `recW4sJlpnCdZAZJ0` | Missing entity link | Link or keep excluded; do not bulk-approve |
| 2025 Mexico Curio FDD (contaminated) | source | `recIH5lyY8MASnfrp` | 102 Kimpton/IHG facts; quarantined | Keep rejected; no re-approve without clean re-extract |
| Curio duplicate fact sheets | sources | `recL1qfHCOAUZr9Rz`, `recMuN9bR1doJ3gjN` | Duplicate captures | Pick one canonical row for future approval |
| Curio excluded noisy sources (14) | diagnostics | various | `approved_for_explorer_use_no` | Cleanup/archive; do not block publish scope |

---

## 10. Notes from Arbor, Kimpton, and Curio Pilots

### Arbor Lodging (operator — clean path)

- **Record:** `recF5Z87OAqFgndoq`
- **Outcome:** 7 approved Explorer-use sources; 1 approved fact → **eligible** with sparse warning; governance **applied**; chip **Source-Informed Profile** · CALA-specific.
- **Confidence:** **Medium** (sparse publish scope).
- **Skipped:** Source Type `Unknown` not written.
- **Lesson:** Operator confidence writes **Data Confidence Level** column alias.
- **Reports:** `reports/arbor-pi-stewardship-pilot.md`, `reports/partner-intelligence-profile-governance-publish.md`

### Kimpton Hotels (brand — clean path)

- **Record:** `recCKuXCmGvxHPfb3`
- **Outcome:** 4 approved company-material sources; 4 approved facts (identity + substantive) → **High** confidence proposal; chip **AI-Assisted Profile**.
- **Lesson:** Simplest brand path when entity link is correct and origins are consistent within publish scope.
- **Plan:** [kimpton-pi-governance-stewardship-plan.md](./kimpton-pi-governance-stewardship-plan.md)

### Curio Collection by Hilton (messy recovery path)

- **Record:** `receQkxgjlezsc1xg`
- **Starting state:** 15 sources, 144 facts, Mexico FDD Kimpton/IHG contamination, mixed origins, zero approved facts.
- **Recovery steps (completed):** Quarantine contaminated facts → narrow clean re-extract (`curio-clean-reextract`) → extraction context fix (Kimpton blind fallback) → approve **1 US FDD source** + **2 identity facts** → publish-scope eligibility → governance **applied** at **Medium** confidence.
- **Final publish scope:** 1 approved source, 2 identity facts, 14 excluded sources (diagnostics only).
- **Lesson:** Do not bulk-steward noisy packages; validate identity values before approve; full-package mixed origins do not block narrow publish scope.
- **Plans:** [curio-pi-package-integrity-cleanup-plan.md](./curio-pi-package-integrity-cleanup-plan.md), [curio-clean-reextraction-plan.md](./curio-clean-reextraction-plan.md), [curio-extraction-context-audit.md](./curio-extraction-context-audit.md)

---

## Batch Cadence (recommended)

| Week focus | Target | Success criterion |
|------------|--------|-------------------|
| Batch 0 (done) | Arbor, Kimpton, Curio | Pipeline proven; 3 profiles at `no_op` |
| Batch 1 | Hotel Equities + Best Western Plus | 2 profiles through dry-run; 1 apply if clean |
| Batch 2 | GHL + Aimbridge (if present) | Operator packages inventoried + first eligibility |
| Batch 3 | Hilton Garden Inn + Radisson Blu | Brand packages with company-materials PI sources |
| Batch 4 | CALA census operator (Viento Sur or successor) | CALA-specific chip validated |

---

## Related Docs

| Doc | Purpose |
|-----|---------|
| [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md) | Step-by-step operating runbook |
| [partner-intelligence-stewardship-fix-plan.md](./partner-intelligence-stewardship-fix-plan.md) | Per-package stewardship guidance |
| [partner-intelligence-profile-governance-publish-plan.md](./partner-intelligence-profile-governance-publish-plan.md) | Publish script design + field mapping |
| [reports/partner-intelligence-publish-readiness.md](../../reports/partner-intelligence-publish-readiness.md) | Latest audit (regenerate before each batch) |

**Last audit snapshot (2026-07-06):** 5 packages, 3 eligible, 2 blocked (Curio orphan sources).
