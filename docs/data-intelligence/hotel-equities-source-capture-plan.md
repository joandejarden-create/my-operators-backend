# Hotel Equities — Source Capture Plan

**Date:** 2026-07-06  
**Status:** **Capture complete** (3 Source Library rows, 2026-07-06) — **next:** [hotel-equities-extraction-plan.md](./hotel-equities-extraction-plan.md) (read-only preview run; no apply extraction yet)
**Parent plan:** [hotel-equities-pi-production-plan.md](./hotel-equities-pi-production-plan.md)

> **Authority:** [DATA_VALIDATION_PROTOCOL.md](./DATA_VALIDATION_PROTOCOL.md), [CONTENT_QA_CHECKLIST.md](./CONTENT_QA_CHECKLIST.md), [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [partner-reference-material-collection-guide.md](../partner-reference-material-collection-guide.md)

---

## 1. Target Operator

| Field | Value |
|-------|-------|
| **Entity** | Hotel Equities (CALA) |
| **Entity type** | Operator |
| **Table** | Operator Setup - Master |
| **Record ID** | `recWPKu5laVZxsvpn` |
| **PI package key** | `operator:recWPKu5laVZxsvpn` |

**Scope note:** PI sources and facts link to the **CALA division** Master row. Corporate Hotel Equities pages still apply as parent-company evidence for this package; region fields should emphasize **CALA** where the source is division-specific.

---

## 2. Current PI Status (2026-07-06)

| Metric | Count |
|--------|-------|
| Source Library rows linked to operator | **3** |
| Extracted Facts | **0** |
| Approved for Explorer Use | **0** |
| Approved / Edited facts | **0** |
| Publish readiness package | **Not eligible** |

**Registered sources (Captured, Explorer use = No):**

| Source ID | Title | URL |
|-----------|-------|-----|
| `rectG9wdsAeL7u0FG` | Hotel Equities home | https://www.hotelequities.com/ |
| `rec9FSzLhaLPcPvtv` | Hotel Equities Services | https://www.hotelequities.com/services.htm |
| `recy1oDTNe7kyQGbE` | Hotel Equities CALA | https://www.hotelequities.com/cala.htm |

**Stewardship dry-run blockers:** `source_status_not_ready:Captured`, `no_approved_explorer_sources`, `no_approved_facts`

**Next:** HTML normalization fixed (2026-07-06) — see [hotel-equities-extraction-plan.md](./hotel-equities-extraction-plan.md). Build narrow apply script before extraction apply.

Report: `reports/partner-intelligence-stewardship-package.md`

---

## 3. Source Capture Priority Table

| P | URL | Capture type | Subfolder | Region | Source origin (planned) | Explorer use (later) |
|---|-----|--------------|-----------|--------|-------------------------|----------------------|
| **P0** | https://www.hotelequities.com/ | Website capture | `website/` | Americas / Global | Public Web or Operator Provided | After human review |
| **P0** | https://www.hotelequities.com/services.htm | Website capture | `website/` | Americas | Public Web | After human review |
| **P0** | https://hotelequities.com/cala.htm | Website capture | `website/` or `regional/` | **CALA** | Operator Provided (division page) | After human review |
| P1 | About / company profile (if distinct URL on hotelequities.com) | Website capture | `website/` | Americas | Public Web | Optional |
| P2 | Official press/news (hotelequities.com only) | Press / website | `press/` | Americas | Public Web | Supporting only — not governance lead |
| P2 | Downloadable PDF brochure/deck (if found on official site) | Operator deck | `operator-materials/` | CALA or Americas | Operator Provided | After human review |

**Do not capture for first governance path:** third-party articles, OTA listings, LinkedIn posts, census operator strings, or Explorer fixture JSON (research seed ≠ PI evidence).

---

## 4. What Each Source Is Expected to Support

| Governance area | P0 home | P0 services | P0 CALA page |
|-----------------|---------|-------------|--------------|
| **Company name** | Corporate name; CALA division naming | HE management brand | **Hotel Equities (CALA)** division label |
| **Overview** | Enterprise positioning, history | Services summary | CALA division mission / 2025 launch context |
| **Services** | High-level capabilities | **Primary** — management, accounting, development services | Resort/lifestyle, openings, transitions |
| **Management model** | Owner-aligned partnership framing | Third-party management scope | Regional operating platform vs remote-only |
| **CALA relevance** | Corporate backing | May reference regions | **Primary** — Caribbean & Latin America scope |
| **Geography** | U.S. / international footprint hints | Service markets | **CALA markets** (DR, México, T&C, etc. if stated) |
| **Brand relationships** | Only if **directly stated** (Marriott/Hilton/Hyatt) | Flag management references | CALA pipeline brands if named |
| **Owner value proposition** | “Create value for ownership” / owner language | Owner-facing service outcomes | Regional owner alignment, local execution |

Steward must reject extracted values that are vague marketing superlatives without a citable sentence on the captured page.

---

## 5. Local Folder Path Recommendation

**Reference root (env):** `PARTNER_REFERENCE_ROOT` → default:

```text
G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material
```

**Company folder (dry-run verified 2026-07-06):**

```text
G:\My Drive\Dealality™\Platform Design & Build\Brand Reference Material\Hotel Equities\
```

**Subfolders to create (via `init-folder --apply` when approved):**

| Subfolder | Hotel Equities use |
|-----------|-------------------|
| `website/` | P0 HTML captures (home, services, CALA) |
| `regional/` | Optional duplicate/archive for CALA-specific PDFs |
| `operator-materials/` | Official decks/PDFs if discovered |
| `press/` | Company-controlled news only (P2) |
| `inbox/` | Uncategorized until sorted |
| `development/`, `fdd/`, `brands/` | Not expected for operator capture v1 |

**Relative paths** for Source Library `Local File Path` column: e.g. `Hotel Equities/website/Hotel Equities home - 2026-07-06.html`

**PI read path (2026-07-06):** `readLocalSourceText()` resolves relative paths against **Brand Reference Material** first, then **Operator Reference Material** if the file is not found under the brand root. Existing rows do not need root prefixes in `Local File Path`. Hotel Equities website HTML remains under Brand Reference Material; unregistered PDFs under Operator Reference Material (`Hotel Equities CALA/`) — see [hotel-equities-pdf-enrichment-plan.md](./hotel-equities-pdf-enrichment-plan.md).

---

## 6. Source Library Registration Plan

After file capture (`--apply`), register rows **without** auto-approval:

| Step | Action | Field values |
|------|--------|--------------|
| 1 | Create row per captured file | Profile Type = **Operator** |
| 2 | Link operator | **Operator / Management Company** → `recWPKu5laVZxsvpn` |
| 3 | Title | Human-readable page title |
| 4 | Source URL | Canonical URL captured |
| 5 | Local File Path | Relative path under reference root |
| 6 | Source Type | **Website Capture** (HTML) or **Operator Capability Deck** (PDF) |
| 7 | Source Origin | **Public Web** (corporate site) or **Operator Provided** (CALA division) |
| 8 | Region | **CALA** for `cala.htm`; **Americas** for corporate pages |
| 9 | Status | **Captured** (not Approved) |
| 10 | Approved for Extraction | **No** until steward review |
| 11 | Approved for Explorer Use | **No** until steward review |
| 12 | Source Quality | **Medium** default; **High** only for dense official deck/PDF |

**CLI registration (when approved — writes Airtable):**

```bash
npm run partner-reference:download -- \
  --url "https://www.hotelequities.com/services" \
  --company "Hotel Equities" \
  --type website-capture \
  --title "Hotel Equities Services" \
  --operator-id recWPKu5laVZxsvpn \
  --profile-type Operator \
  --apply --register
```

Repeat per P0 URL. **Do not** set Approved for Explorer Use or run extraction until CONTENT_QA + steward sign-off.

**Alternative:** Manual Source Library row creation in Airtable with same field mapping.

---

## 7. Target Fact Keys for Extraction

Prioritize registry keys from `lib/partner-intelligence/operator-explorer-registry-catalog.js` and stewardship governance list:

| Priority | Field key | Source expectation |
|----------|-----------|-------------------|
| P0 | `op.snapshot.companyName` | “Hotel Equities” / “Hotel Equities (CALA)” as stated |
| P0 | `op.snapshot.companyDescription` | 1–3 sentence overview from home or CALA page |
| P1 | `op.snapshot.parentCompany` | Only if page states parent (e.g. division of Hotel Equities) — **do not infer** |
| P0 | `op.platform.offeredServices` | Services page bullet list / capability summary |
| P0 | `op.capabilities.managementServices` | Third-party management scope |
| P0 | `op.geography.regions` | CALA / Caribbean / Latin America as stated |
| P1 | `op.brandRelationships` | Only if Marriott, Hilton, Hyatt (or other flags) **named on source** |
| P0 | `op.ownerValueProposition` | Owner-facing value language (directly stated) |
| P0 | `op.operatingModel` | Partnership / management model phrasing |

**First publish-scope target:** 3–8 approved facts spanning identity + at least one capabilities/geography/operating-model field (required for High confidence per sparse-confidence rules).

---

## 8. Stewardship Rules

1. **Approve only official sources first** — P0 URLs above; no third-party press for initial publish scope.
2. **Approve only 3–8 clean facts** — governance-priority keys; no duplicate field keys unless edited correction.
3. **Reject vague marketing** — superlatives without evidence text; “world-class”, “leading” without support.
4. **Validate identity** — company name on facts must match Hotel Equities / HE CALA; no cross-operator leakage.
5. **Do not set Company Validated** — PI path never sets checkbox or Company Validation Date.
6. **Do not publish profile governance** until:
   - `npm run audit-partner-intelligence-publish-readiness` shows publish-scope **eligible**
   - `npm run publish-partner-intelligence-profile-governance -- --dry-run` reviewed
   - Founder explicit `--apply` approval
7. **Excluded sources stay diagnostic** — if later sources are linked but not Explorer-approved, they must not block a narrow publish scope (same rule as Curio).
8. **Quarantine contamination** — if extraction returns wrong operator/brand names, reject facts; do not bulk-approve.

---

## 9. Exact Next Commands

### Already run (dry-run — safe)

```bash
npm run partner-reference:init-folder -- --company "Hotel Equities"
```

Default is dry-run without `--apply`. Output confirmed folder plan under Brand Reference Material.

### Next — folder create (human approval required)

```bash
npm run partner-reference:init-folder -- --company "Hotel Equities" --apply
```

### Next — per-URL download dry-run (no writes)

```bash
npm run partner-reference:download -- --url "https://www.hotelequities.com/" --company "Hotel Equities" --type website-capture --title "Hotel Equities home" --operator-id recWPKu5laVZxsvpn --profile-type Operator

npm run partner-reference:download -- --url "https://www.hotelequities.com/services" --company "Hotel Equities" --type website-capture --title "Hotel Equities Services" --operator-id recWPKu5laVZxsvpn --profile-type Operator

npm run partner-reference:download -- --url "https://hotelequities.com/cala.htm" --company "Hotel Equities" --type website-capture --title "Hotel Equities CALA" --operator-id recWPKu5laVZxsvpn --profile-type Operator
```

### After capture + registration (still no approval)

```bash
# Read-only extraction preview (safe — no Airtable writes)
node scripts/hotel-equities-extract-preview.mjs

# Stewardship (dry-run only until facts exist)
npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute
```

**Extraction apply:** `npm run hotel-equities-extract -- --dry-run` first; apply only with `--apply --approve-hotel-equities-extract` after founder review.

---

## 10. Script Inventory & Gaps

| Script | Safe for HE today? | Notes |
|--------|-------------------|-------|
| `partner-reference:init-folder` | **Yes** (dry-run default) | Ran 2026-07-06 |
| `partner-reference:download` | **Yes** (dry-run default) | HTML fetch + optional `--apply --register` |
| `partner-reference:harvest-operators` | **No profile yet** | Requires `hotelEquities` entry in `operator-reference-registry.js` (Arbor/Brittain pattern) |
| `seed-arbor-pilot-sources.mjs` | **No HE variant** | Recommend `seed-hotel-equities-pilot-sources.mjs` after P0 URLs validated |
| `steward-partner-intelligence` | **Yes** (dry-run) | Empty until sources exist |

**Recommended next implementation (code — separate task):**

1. Add `hotelEquities` harvest profile to `lib/partner-intelligence/operator-reference-registry.js` with P0 URLs + `operatorId: recWPKu5laVZxsvpn`.
2. Dry-run: `npm run partner-reference:harvest-operators -- --operator hotelEquities` (no `--apply` first).
3. Optional: `seed-hotel-equities-pilot-sources.mjs` mirroring Arbor for Source Library seed dry-run.

Until harvest profile exists, use **`partner-reference:download`** per URL (dry-run → apply → register when approved).

---

## 11. Expected Governance Outcome (after clean capture + review)

| Outcome | Likelihood |
|---------|------------|
| External chip | **Source-Informed Profile** (public official web) or **AI-Assisted Profile** (if mapped conservatively from company materials) |
| Validation Status (internal) | **Source-Informed** or **Company Published** — not **Company Validated** |
| Confidence Level | **Medium** until ≥3 substantive approved facts |
| Source Region | **CALA-Specific** for this Master row |
| Company Validated | **Never** from PI alone |

---

## Related

| Doc / report | Path |
|--------------|------|
| Production plan | [hotel-equities-pi-production-plan.md](./hotel-equities-pi-production-plan.md) |
| Extraction plan | [hotel-equities-extraction-plan.md](./hotel-equities-extraction-plan.md) |
| Priority tracker | [partner-intelligence-priority-profile-production-tracker.md](./partner-intelligence-priority-profile-production-tracker.md) |
| URL catalog | `scripts/brand-reference-material-companies.json` (`hotel_equities`) |
| Stewardship report | `reports/partner-intelligence-stewardship-package.md` |

**Last updated:** 2026-07-06
