# Operator Explorer — Brand Research Reuse Audit

**Date:** 2026-08-09  
**Mode:** Implementation inspection (not docs-only)  
**Companion:** `docs/audits/operator-intelligence-brand-explorer-process-audit.md` (2026-08-03) — extended here for Operator Explorer architecture

---

## Pipeline map (Brand Explorer → files)

| Stage | Brand Explorer component | Primary files / modules |
| ----- | ------------------------ | ----------------------- |
| Brand Input | Wave manifests, Active universe SoT | `brand-explorer-wave*-manifest.js`, `brand-explorer-active-universe-source-of-truth.js` |
| Entity Resolution | Brand identity anchors / slug maps | `EXTRA_ACTIVE_IDENTITY_ANCHORS` patterns, universe SoT |
| Source Discovery | Source packs + lifestyle capture | `brand-explorer-*-source-packs*.js`, `brand-explorer-lifestyle-affiliation-source-capture*.js` |
| Source Ranking | Governance docs + provenance audits | `docs/data-intelligence/SOURCE_RANKING_GUIDE.md`, `brand-explorer-source-provenance-by-tab.js` |
| Source Capture | PI Source Library writers | `api/lib/partner-intelligence-field-map.js`, source registry writers |
| Claim Extraction | Fact extraction / Webhound claim patches | Extracted Facts table; `brand-explorer-62-webhound-*-claims*.js` (brand-specific) |
| Normalization | Field maps + taxonomy sanitizers | Presentation sanitizers, flexibility sanitizer, fee/FDD normalizers |
| Evidence Classification | Evidence class + validation levels | `INTELLIGENCE_GOVERNANCE.md`, `DATA_VALIDATION_PROTOCOL.md` |
| Conflict Detection | Limited brand-side; stronger on operator intel | Operator: `lib/operator-intelligence/conflict-detector.js` |
| Publication Decision | External display gating + publish queues | `controlled-publish` patterns; OE: `lib/operator-intelligence/publication-policy.js` |
| Airtable Write Plan | Factory apply-draft / wave apply | `brand-explorer-active-profile-factory`, wave `*-apply` scripts |
| Backup | Apply manifests / backups | Wave backup JSON under `reports/` |
| Apply | Dry-run then `--apply` | OS patch safety: `brand-explorer-os-patch-safety.js` |
| Post-Write Validation | PVQL, completeness, image audits | `test:brand-explorer-public-visibility-quality-lock`, rendered-field audits |
| Explorer Readiness | Tab Factory + OS gates + freeze | `brand-explorer-os-gate-evaluator.js`, baseline freeze scripts |
| Exception Queue | Founder visual review packets | `brand-explorer-os-founder-review-packet.js`, action router |
| Refresh / Staleness | Wave/audit triggered (not continuous loop) | Refresh Due / Last Reviewed fields; wave remediations |

---

## Component reuse table

| Brand Explorer Component | Current File/Module | Generic? | Brand-Specific? | Reusable for Operators? | Refactor Needed? |
| ------------------------ | ------------------- | -------- | --------------- | ----------------------- | ---------------- |
| Intelligence governance | `docs/data-intelligence/INTELLIGENCE_GOVERNANCE.md` | Yes | No | **Direct reuse** | No |
| Source ranking guide | `SOURCE_RANKING_GUIDE.md` | Mostly | CALA examples | **Adapt** | Small |
| PI Source Library | `partner-intelligence-field-map.js`, Source Library table | Yes | Operator link already exists | **Direct reuse** | No |
| Extracted Facts / Published Fields | PI tables | Yes | Presentation targets differ | **Direct reuse** + operator destinations | Small |
| Dry-run → apply → validate | `brand-explorer-os-*`, wave factories | Pattern | Brand gates | **Adapt** (OE already has `operator-explorer-os.js`) | Adapter |
| Tab Factory + release gates | Brand + OE tab factories | Pattern | Brand tab contracts | **Adapt** (OE exists) | Keep separate contracts |
| Protected baseline freeze | Brand 62 Active/Live; OE Arbor+HE | Pattern | Brand universe | **Pattern only** | Separate freezes |
| Publication policy | Brand display gating | Partial | Brand economics | Operator already has `publication-policy.js` | Align naming |
| Conflict detection | Sparse on brand | Partial | — | Prefer operator `conflict-detector.js` | Extend |
| Claim model | Brand facts / Webhound patches | Concept | Brand claim writers | Prefer **Operator Intelligence - Claims** | Do not merge writers |
| Wave processing | `brand-explorer-wave*-factory.js` | Pattern | Per-brand packs | **Adapt** to operator wave input | New operator adapter |
| Exception / founder review | Founder packets | Process | Brand visual QA | **Adapt** — policy + exceptions, not every fact | Yes |
| Backup / rollback manifests | reports + apply scripts | Yes | — | **Direct reuse pattern** | No |
| Image uniqueness / Scene7 | `image-uniqueness-v2`, gallery selection | No | Yes | **Do not reuse** | N/A |
| Flexibility taxonomy | Brand PIP flexibility | No | Yes | **Do not reuse** | N/A |
| FDD / fee stacks | Brand Setup Fee / FDD tables | No | Yes | **Do not reuse** for OE research | N/A |
| PRIMARY_RELEASE lists | Historical brand overlays | No | Yes | **Do not reuse** | N/A |
| Brand Status Active/Live SoT | Brand Basics status | No | Yes | Operators use `submission_status` | Keep separate |
| Per-brand content writers | Hundreds of `*-writer.js` | No | Yes | Reuse **shape**, not content | Temporary duplication OK |
| Gallery / momentum cards | Openings, Scene7, Hilton CDN rules | No | Yes | Reuse **card shape** only | Adapter |
| Batch learning ledger | Census/Brand learning loop | Partial | Brand/census | Optional later for operators | Later |

---

## Exclusion list (must remain Brand Explorer specific)

| Exclusion | Why |
| --------- | --- |
| Brand Status Active/Live universe + 62 public-full freeze | Different entity lifecycle; operators use Master `submission_status` |
| Scene7 / Hilton `impolicy` / image uniqueness v2 | Franchise DAM assumptions; operators lack equivalent photo DAM |
| Flexibility Indicators taxonomy | Brand franchise product attribute |
| FDD / Item 19 / fee-stack publication rules | Franchise disclosure economics — not third-party operator Explorer |
| PRIMARY_RELEASE / lane restore brand lists | Operational brand overlays; not operator SoT |
| Value Creation Scenarios / overview scenario owner-value bar | Brand owner-economics narrative pattern; OE needs different sections |
| Brand-chain-scale publication rules tied to Brand Setup | Wrong master entity |
| Webhound as production writer | Hard-case learning only (AGENTS.md) — same rule for operators |
| Per-brand identity anchors (`EXTRA_ACTIVE_IDENTITY_ANCHORS`) | Brand-specific PVQL; OE uses Master record IDs + slugs |

---

## Recommendation: shared research core?

**Prefer: Shared Intelligence Research Core (thin) + Brand Adapter + Operator Adapter.**

Do **not** merge Brand Explorer OS and Operator Explorer OS into one state machine — presentation contracts and universes differ.

| Component | Recommendation |
| --------- | -------------- |
| Source discovery / ranking / PI capture | Direct reuse |
| Publication policy / conflict / claim spine | Operator modules already exist — extend; brand can later converge |
| Wave orchestration | Small extraction of dry-run/apply/backup/validate shell |
| Tab Factory / Explorer presentation | Adapter / parallel (already) |
| Brand writers / image / FDD | Duplication temporarily safer — never share |

---

## Validation / readiness scripts (Brand + Operator)

**Brand:** `brand-explorer-os`, `test:brand-explorer-public-visibility-quality-lock`, baseline freeze, tab-factory, provenance, image uniqueness  

**Operator:** `operator-explorer-os`, `test:operator-explorer-quality-baseline`, `test:operator-explorer-mandatory-release-gates`, tab-factory / section-pattern / provenance audits, Fit readiness scripts

---

## Wave / batch command structure (Brand pattern to mirror)

```text
preflight → manifest/source-packs → build-draft → dry-run apply → gates → founder exceptions → apply → post-write validate → freeze/readiness
```

Operator Intelligence already used a lighter variant: seed → evaluate → dry-run write plan → apply → post-write validate.
