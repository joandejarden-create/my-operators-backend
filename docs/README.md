# Dealality Documentation Index

Internal documentation for Dealality builds, data governance, and platform reference.

## Folder Structure

```text
docs/
  ai-build-system/      # How AI and Cursor build Dealality
  data-intelligence/    # Validated data and intelligence governance
  platform-reference/   # Data dictionary, Deals reference, architecture/testing placeholders
  archive/              # Reserved for clearly outdated docs (empty — nothing archived yet)
  gtm-resources/        # Existing GTM pilot resources
  sample-deals/         # Existing sample deal build reports
  [~130 root-level docs] # Existing operational docs (unchanged)
```

## Start Here (AI Builds)

| Document | Purpose |
|----------|---------|
| [ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md](./ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md) | Permanent product principles |
| [ai-build-system/AI_BUILD_PROTOCOL.md](./ai-build-system/AI_BUILD_PROTOCOL.md) | Standard AI build flow and rules |
| [ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md](./ai-build-system/CURSOR_IMPLEMENTATION_PROTOCOL.md) | Cursor-specific implementation steps |
| [ai-build-system/OLD_HOME_ANTI_GLITCH_RULES.md](./ai-build-system/OLD_HOME_ANTI_GLITCH_RULES.md) | Old Home FOUC: bake final chrome before reveal |
| [ai-build-system/CURSOR_PROMPTS.md](./ai-build-system/CURSOR_PROMPTS.md) | Copy-paste prompts for builds and QA |
| [ai-build-system/BUILD_DECISIONS.md](./ai-build-system/BUILD_DECISIONS.md) | Living decision log |

## Data & Intelligence

| Document | Purpose |
|----------|---------|
| [data-intelligence/dealality-batch-learning-system.md](./data-intelligence/dealality-batch-learning-system.md) | **Batch learning loop** — Census + Brand Explorer code improvement ledger (`npm run dealality:batch-learning-ledger` · `npm run dealality:batch-learning-audit`) |
| [data-intelligence/INTELLIGENCE_GOVERNANCE.md](./data-intelligence/INTELLIGENCE_GOVERNANCE.md) | Primary governance for platform intelligence |
| [data-intelligence/DATA_VALIDATION_PROTOCOL.md](./data-intelligence/DATA_VALIDATION_PROTOCOL.md) | How data enters the platform |
| [data-intelligence/SOURCE_RANKING_GUIDE.md](./data-intelligence/SOURCE_RANKING_GUIDE.md) | Source trust and regional relevance |
| [data-intelligence/CONTENT_QA_CHECKLIST.md](./data-intelligence/CONTENT_QA_CHECKLIST.md) | Content QA before publish |
| [data-intelligence/brand-operator-validation-fields-plan.md](./data-intelligence/brand-operator-validation-fields-plan.md) | **Plan** — Brand/Operator validation & governance fields |
| [data-intelligence/governance-read-path-trust-label-plan.md](./data-intelligence/governance-read-path-trust-label-plan.md) | **Plan** — API read path + Explorer trust labels |
| [data-intelligence/partner-intelligence-profile-governance-publish-plan.md](./data-intelligence/partner-intelligence-profile-governance-publish-plan.md) | **Plan + publish script** — PI → Setup profile governance (`npm run publish-partner-intelligence-profile-governance`, dry-run default) |
| [data-intelligence/kimpton-pi-governance-stewardship-plan.md](./data-intelligence/kimpton-pi-governance-stewardship-plan.md) | **Plan** — Kimpton brand PI → governance stewardship (dry-run assessment; manual cleanup) |
| [data-intelligence/partner-intelligence-profile-governance-runbook.md](./data-intelligence/partner-intelligence-profile-governance-runbook.md) | **Runbook** — repeatable PI → trust chip pipeline (Arbor + Kimpton + Curio pilots completed) |
| [data-intelligence/partner-intelligence-priority-profile-production-tracker.md](./data-intelligence/partner-intelligence-priority-profile-production-tracker.md) | **Tracker** — next-batch Brand/Operator PI profile production (status table + checklists) |
| [data-intelligence/active-brand-governance-upgrade-v1.md](./data-intelligence/active-brand-governance-upgrade-v1.md) | **Audit** — existing Explorer-active brands governance upgrade dry-run (`npm run active-brand-governance-upgrade -- --dry-run`) |
| [data-intelligence/choice-legacy-brand-source-package-v1.md](./data-intelligence/choice-legacy-brand-source-package-v1.md) | **Plan** — Choice legacy brand P0/P1 source packages (`npm run choice-legacy-brand-source-package -- --dry-run`) |
| [data-intelligence/choice-legacy-brand-mini-batch-1.md](./data-intelligence/choice-legacy-brand-mini-batch-1.md) | **Batch** — Comfort/Everhome/Quality mini-batch (`npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-1 --dry-run`) |
| [data-intelligence/choice-legacy-batch-source-stewardship-v1.md](./data-intelligence/choice-legacy-batch-source-stewardship-v1.md) | **Stewardship** — batch source approval (`npm run choice-legacy-batch-source-stewardship -- --batch <mini-batch-1\|mini-batch-2> --dry-run`) |
| [data-intelligence/choice-legacy-batch-url-capture-v1.md](./data-intelligence/choice-legacy-batch-url-capture-v1.md) | **URL capture** — batch consumer + press pages (`npm run choice-legacy-batch-url-capture -- --batch <mini-batch-1\|mini-batch-2> --dry-run`) |
| [data-intelligence/choice-legacy-batch-extraction-v1.md](./data-intelligence/choice-legacy-batch-extraction-v1.md) | **Extraction** — batch fact extraction (`npm run choice-legacy-batch-extract -- --batch <mini-batch-1\|mini-batch-2> --dry-run`) |
| [data-intelligence/choice-legacy-batch-fact-stewardship-v1.md](./data-intelligence/choice-legacy-batch-fact-stewardship-v1.md) | **Fact stewardship** — batch Pending fact review (`npm run choice-legacy-batch-fact-stewardship -- --batch <mini-batch-1\|mini-batch-2> --dry-run`) |
| [data-intelligence/choice-legacy-batch-governance-publish-v1.md](./data-intelligence/choice-legacy-batch-governance-publish-v1.md) | **Governance publish** — batch trust-label publish (`npm run choice-legacy-batch-governance-publish -- --batch <mini-batch-1\|mini-batch-2> --dry-run`) |
| [data-intelligence/choice-legacy-batch-pipeline-v1.md](./data-intelligence/choice-legacy-batch-pipeline-v1.md) | **Pipeline orchestrator** — end-to-end batch run (`npm run choice-legacy-batch-pipeline -- --batch <batch> --dry-run`) |
| Ascend source gap (resolved) | `npm run ascend-source-gap-resolution` → `reports/ascend-source-gap-resolution.{md,json}` · mini-batch-3 pipeline |
| [data-intelligence/tribute-portfolio-brand-package-pilot.md](./data-intelligence/tribute-portfolio-brand-package-pilot.md) | **Full package pilot (non-Choice)** — Tribute Portfolio by Marriott (`recCvV0PuZOi8c3hC`); `npm run tribute-portfolio-brand-package -- --dry-run` → `reports/tribute-portfolio-brand-package.{md,json}`. Uses reusable `brand-source-auto-resolver.js`. |
| [data-intelligence/tribute-portfolio-package-pipeline-v1.md](./data-intelligence/tribute-portfolio-package-pipeline-v1.md) | **One-command Tribute pipeline** — `npm run tribute-portfolio-package-pipeline -- --dry-run` orchestrates registration → stewardship → extraction → fact stewardship → governance publish → verification (reuses factory primitives + auto-resolver). Apply gated behind `--approve-tribute-portfolio-package-pipeline`. → `reports/tribute-portfolio-package-pipeline.{md,json}`. |
| [data-intelligence/brand-asset-pr-package-governance-v1.md](./data-intelligence/brand-asset-pr-package-governance-v1.md) | **Asset & PR governance v1 (read-only)** — Tribute visual-package gap audit; `npm run brand-asset-pr-package-governance -- --brand tribute-portfolio --dry-run` → `reports/brand-asset-pr-package-governance.{md,json}`. No image downloads or Airtable writes. |
| [data-intelligence/brand-asset-registry-workflow-v1.md](./data-intelligence/brand-asset-registry-workflow-v1.md) | **Asset registry / approval workflow v1** — proposes Brand Asset Registry schema + stages Tribute candidates; `npm run brand-asset-registry-workflow -- --brand tribute-portfolio --dry-run`. Schema apply gated. |
| [data-intelligence/brand-explorer-visual-slot-requirements-v1.md](./data-intelligence/brand-explorer-visual-slot-requirements-v1.md) | **Visual slot requirements v1** — defines slot-specific image requirements (logo/hero/gallery/openings/value driver/PR) + audits registry records; `npm run brand-explorer-visual-slot-requirements -- --brand tribute-portfolio --dry-run`. Schema apply gated; status corrections report-only. |
| [data-intelligence/cala-tribute-property-visual-discovery-v1.md](./data-intelligence/cala-tribute-property-visual-discovery-v1.md) | **CALA Tribute property visual discovery v1** — finds named CALA Tribute hotel/property image candidates from Marriott sitemaps; `npm run cala-tribute-property-visual-discovery -- --dry-run`. Registry staging gated. |
| [data-intelligence/tribute-visual-asset-slot-review-v3.md](./data-intelligence/tribute-visual-asset-slot-review-v3.md) | **Tribute visual slot review v3** — groups registry candidates by Explorer slot, scores competing candidates, recommends primary/alternate; `npm run tribute-visual-asset-slot-review -- --dry-run`. Selection apply gated. |
| [data-intelligence/brand-asset-human-review-readiness-v4.md](./data-intelligence/brand-asset-human-review-readiness-v4.md) | **Brand asset human review readiness v4** — evaluates whether primary candidates have enough metadata for human usage review; `npm run brand-asset-human-review-readiness -- --brand tribute-portfolio --dry-run`. Report-only. |
| [data-intelligence/brand-asset-review-decision-writer-v5.md](./data-intelligence/brand-asset-review-decision-writer-v5.md) | **Brand asset review decision writer v5/v5.1** — applies explicit human approval/rejection and audits approval-state consistency; `npm run brand-asset-review-decision-writer -- --brand tribute-portfolio --dry-run`. Apply gated with `--approve-records` or `--approve-brand-asset-approval-state-corrections`. |
| [data-intelligence/brand-asset-download-attachment-writer-v6.md](./data-intelligence/brand-asset-download-attachment-writer-v6.md) | **Brand asset download & attachment writer v6** — downloads and stages only formally approved assets, then patches Brand Asset Registry Attachment/Local File Path metadata; `npm run brand-asset-download-attachment-writer -- --brand tribute-portfolio --dry-run`. Apply gated with `--approve-brand-asset-download-attachments`. |
| [data-intelligence/explorer-media-promotion-writer-v7.md](./data-intelligence/explorer-media-promotion-writer-v7.md) | **Explorer media promotion writer v7** — promotes only formally approved registry assets into Brand Setup/Presentation media fields with strict overwrite gates; `npm run explorer-media-promotion-writer -- --brand tribute-portfolio --dry-run`. Apply gated with `--approve-explorer-media-promotion`. |
| [data-intelligence/brand-explorer-visual-qa-verification-v8.md](./data-intelligence/brand-explorer-visual-qa-verification-v8.md) | **Brand Explorer visual QA verification v8** — read-only end-to-end check that promoted slot images are readable by API/frontend; `npm run brand-explorer-visual-qa-verification -- --brand tribute-portfolio --dry-run`. |
| [data-intelligence/brand-explorer-presentation-copy-parity-audit-v8-5.md](./data-intelligence/brand-explorer-presentation-copy-parity-audit-v8-5.md) | **Presentation copy parity audit v8.5** — compares Tribute copy against reference profiles and proposes source-backed/owner-facing copy model; `npm run brand-explorer-presentation-copy-parity-audit -- --brand tribute-portfolio --dry-run`. |
| [data-intelligence/brand-explorer-presentation-copy-promotion-writer-v9.md](./data-intelligence/brand-explorer-presentation-copy-promotion-writer-v9.md) | **Presentation copy promotion writer v9** — copy-only writer for existing promoted slots (`Title`/`Body`), dry-run by default with apply and overwrite gates; `npm run brand-explorer-presentation-copy-promotion-writer -- --brand tribute-portfolio --dry-run`. |
| [data-intelligence/brand-explorer-value-driver-copy-parity-fix-v9-1.md](./data-intelligence/brand-explorer-value-driver-copy-parity-fix-v9-1.md) | **Value-driver copy parity fix v9.1** — focused update for Tribute `overview.scenario.1` and `.2` to remove property-specific phrasing and enforce strategic owner-facing language; `npm run brand-explorer-value-driver-copy-parity-fix -- --brand tribute-portfolio --dry-run`. |
| [data-intelligence/tribute-brand-explorer-content-parity-audit-v10.md](./data-intelligence/tribute-brand-explorer-content-parity-audit-v10.md) | **Full Brand Explorer content parity audit v10** — read-only section/field parity + completion plan for Tribute against completed brands; `npm run tribute-brand-explorer-content-parity-audit -- --dry-run`. |
| [data-intelligence/hotel-equities-pi-production-plan.md](./data-intelligence/hotel-equities-pi-production-plan.md) | **Plan** — Hotel Equities (CALA) PI production readiness (`recWPKu5laVZxsvpn`; source capture first) |
| [data-intelligence/operator-explorer-protected-baseline-rules.md](./data-intelligence/operator-explorer-protected-baseline-rules.md) | **Binding** — Operator Explorer quality baseline rules (Arbor + Hotel Equities) |
| [data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md](./data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md) | **Freeze** — 2-operator golden quality baseline |
| [data-intelligence/operator-explorer-tab-factory-build-operation.md](./data-intelligence/operator-explorer-tab-factory-build-operation.md) | **Tab Factory** — permanent Operator Explorer build sequence (Brand parallel) |
| [data-intelligence/operator-explorer-mandatory-release-gates.md](./data-intelligence/operator-explorer-mandatory-release-gates.md) | **Gates** — founder/active readiness requirements for Operator Explorer |
| [data-intelligence/operator-explorer-section-pattern-parity.md](./data-intelligence/operator-explorer-section-pattern-parity.md) | **Pattern parity** — Arbor/HE section pattern gate |
| [data-intelligence/operator-explorer-source-provenance-by-tab.md](./data-intelligence/operator-explorer-source-provenance-by-tab.md) | **Provenance** — operator-specific source mix by tab |
| [data-intelligence/operator-explorer-ready-for-next-operator.md](./data-intelligence/operator-explorer-ready-for-next-operator.md) | **Next operator** — when OS says start Tab Factory (queue: GHL) |
| [data-intelligence/hotel-equities-source-capture-plan.md](./data-intelligence/hotel-equities-source-capture-plan.md) | **Plan** — Hotel Equities P0 source capture + Source Library registration (`recWPKu5laVZxsvpn`) |
| [data-intelligence/hotel-equities-extraction-plan.md](./data-intelligence/hotel-equities-extraction-plan.md) | **Plan** — HE extraction preview + narrow apply path (read-only preview run; no apply yet) |
| [data-intelligence/hotel-equities-pdf-enrichment-plan.md](./data-intelligence/hotel-equities-pdf-enrichment-plan.md) | **Plan** — HE CALA PDF Source Library registration |
| [data-intelligence/radisson-blu-pi-production-plan.md](./data-intelligence/radisson-blu-pi-production-plan.md) | **Plan** — Radisson Blu (Choice) PI production (`recWPEvxBQxVVzSq3`; Americas/Choice scope) |
| [data-intelligence/radisson-blu-extraction-plan.md](./data-intelligence/radisson-blu-extraction-plan.md) | **Plan** — Radisson Blu narrow extract (`npm run radisson-blu-extract -- --dry-run`) |
| Radisson Blu narrow extract (dry-run default) | `npm run radisson-blu-extract -- --dry-run` → `reports/radisson-blu-extract.{md,json}` |
| [data-intelligence/ghl-hotels-pi-production-plan.md](./data-intelligence/ghl-hotels-pi-production-plan.md) | **Plan** — GHL Hoteles PI production readiness (`reciI2tYQBfMoMK9G`; source capture + extraction) |
| [data-intelligence/ghl-hoteles-extraction-plan.md](./data-intelligence/ghl-hoteles-extraction-plan.md) | **Plan** — GHL narrow extraction allowlist (`npm run ghl-hoteles-extract`) |
| [data-intelligence/dealality-intelligence-production-workflow-v1.md](./data-intelligence/dealality-intelligence-production-workflow-v1.md) | **Workflow** — intelligence production v1 + v1.1 batch queue |
| [data-intelligence/approved-intelligence-platform-field-publishing-v1.md](./data-intelligence/approved-intelligence-platform-field-publishing-v1.md) | **Bridge** — approved facts → platform field audit |
| [data-intelligence/approved-intelligence-field-suggestions-v1.md](./data-intelligence/approved-intelligence-field-suggestions-v1.md) | **Review** — Mode B field suggestions |
| [data-intelligence/controlled-platform-field-publishing-v2.md](./data-intelligence/controlled-platform-field-publishing-v2.md) | **Publish** — guarded single-field write v2 |
| [data-intelligence/controlled-publish-queue-v2-1.md](./data-intelligence/controlled-publish-queue-v2-1.md) | **Queue** — batch controlled publish readiness (`npm run controlled-publish-queue`) |
| [data-intelligence/approved-fact-correction-v1.md](./data-intelligence/approved-fact-correction-v1.md) | **Correct** — steward-reviewed PI fact value correction (`npm run approved-fact-correction`) |
| [data-intelligence/hotel-equities-pdf-manual-curation-plan.md](./data-intelligence/hotel-equities-pdf-manual-curation-plan.md) | **Plan** — HE PDF manual fact curation (preferred over auto-apply) |
| PDF source registration | `npm run register-hotel-equities-pdf-sources -- --dry-run` → `reports/hotel-equities-pdf-register.{md,json}` |
| Radisson Blu PDF source registration | `npm run register-radisson-blu-pdf-sources -- --dry-run` → `reports/radisson-blu-pdf-register.{md,json}` |
| Active brand governance upgrade audit | `npm run active-brand-governance-upgrade -- --dry-run` → `reports/active-brand-governance-upgrade.{md,json}` |
| Choice legacy brand source package | `npm run choice-legacy-brand-source-package -- --dry-run` → `reports/choice-legacy-brand-source-package.{md,json}` |
| Choice legacy mini-batch 1 | `npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-1 --dry-run` → `reports/choice-legacy-brand-mini-batch-1.{md,json}` |
| Choice legacy mini-batch 2 | `npm run choice-legacy-brand-source-package-batch -- --batch mini-batch-2 --dry-run` → `reports/choice-legacy-mini-batch-2-source-package.{md,json}` + `reports/choice-legacy-mini-batch-2-status.{md,json}` |
| Choice legacy batch stewardship | `npm run choice-legacy-batch-source-stewardship -- --batch <batch> --dry-run` → batch-specific stewardship report |
| Choice legacy batch URL capture | `npm run choice-legacy-batch-url-capture -- --batch <batch> --dry-run` → batch-specific URL capture report |
| Choice legacy batch extract | `npm run choice-legacy-batch-extract -- --batch <batch> --dry-run` → batch-specific extract report |
| Choice legacy batch fact stewardship | `npm run choice-legacy-batch-fact-stewardship -- --batch <batch> --dry-run` → batch-specific fact stewardship report |
| Choice legacy batch governance publish | `npm run choice-legacy-batch-governance-publish -- --batch <batch> --dry-run` → batch-specific governance publish report |
| Choice legacy batch pipeline | `npm run choice-legacy-batch-pipeline -- --batch <batch> --dry-run` → `reports/choice-legacy-batch-pipeline.{md,json}` |
| Extraction preview (read-only) | `node scripts/hotel-equities-extract-preview.mjs` → `reports/hotel-equities-extraction-preview.{md,json}` |
| Narrow extraction (dry-run default) | `npm run hotel-equities-extract -- --dry-run` → `reports/hotel-equities-extract.{md,json}` |
| PDF enrichment extract (dry-run) | `npm run hotel-equities-extract -- --dry-run --source-group pdf` |
| PI local file read (dual roots) | `readLocalSourceText()` resolves Brand Reference Material first, then Operator Reference Material — `npm run test:partner-intelligence-extract-source-text` |
| PI stewardship assistant | `npm run steward-partner-intelligence -- --entity-type brand\|operator --target-rec-id rec... --dry-run` → `reports/partner-intelligence-stewardship-package.{md,json}` |
| PI publish readiness audit | `npm run audit-partner-intelligence-publish-readiness` → `reports/partner-intelligence-publish-readiness.{md,json}` (uses `equivalent_stable_live_governance` for cosmetic live-vs-proposal drift) |
| [data-intelligence/partner-intelligence-stewardship-fix-plan.md](./data-intelligence/partner-intelligence-stewardship-fix-plan.md) | **Plan** — PI stewardship fixes from audit; use **generalized assistant** for new packages |
| [data-intelligence/curio-pi-package-integrity-cleanup-plan.md](./data-intelligence/curio-pi-package-integrity-cleanup-plan.md) | **Plan** — Curio package integrity cleanup (**not** a normal stewardship apply — wrong-brand facts + origin conflict) |
| [data-intelligence/curio-clean-reextraction-plan.md](./data-intelligence/curio-clean-reextraction-plan.md) | **Plan** — Curio narrow re-extract from company-material sources only (US FDD + fact sheet); prerequisite for first publish |
| Curio clean re-extract | `npm run curio-clean-reextract -- --dry-run` → `reports/curio-clean-reextract.{md,json}` (apply: `--apply --approve-curio-clean-reextract`) |
| [data-intelligence/curio-extraction-context-audit.md](./data-intelligence/curio-extraction-context-audit.md) | **Audit** — Kimpton/IHG blind fallback root cause + fix (2026-07-06) |
| Curio contaminated fact quarantine | `npm run quarantine-curio-pi-facts -- --dry-run` → `reports/curio-pi-contaminated-facts-quarantine.{md,json}` |
| P1 governance setup (dry-run) | `npm run setup-brand-validation-fields -- --dry-run`, `npm run setup-operator-validation-fields -- --dry-run` |

## Platform Reference

| Document | Purpose |
|----------|---------|
| [platform-reference/DATA_DICTIONARY.md](./platform-reference/DATA_DICTIONARY.md) | **Active v1** — cross-table dictionary and base topology |
| [platform-reference/airtable-deals-fields.md](./platform-reference/airtable-deals-fields.md) | **Focused reference** — Deals table and linked child tables |
| Live schema diff | `npm run audit-airtable-deals-schema` → `reports/airtable-deals-schema-diff.md` |
| [data-intelligence/brand-operator-validation-fields-plan.md](./data-intelligence/brand-operator-validation-fields-plan.md) | Brand/Operator validation field rollout plan |
| Brand/Operator validation audit | `npm run audit-brand-operator-validation-fields` → `reports/brand-operator-validation-schema-diff.md` |
| [platform-reference/ARCHITECTURE_MAP.md](./platform-reference/ARCHITECTURE_MAP.md) | Future system architecture map |
| [platform-reference/TESTING_PROTOCOL.md](./platform-reference/TESTING_PROTOCOL.md) | Future consolidated testing guide |

## Existing Docs (Not Moved)

Root-level `docs/` files remain in place to avoid breaking references in `AGENTS.md`, code comments, and scripts. See [platform-reference/DATA_DICTIONARY.md](./platform-reference/DATA_DICTIONARY.md) for schema doc patterns.

**Suggested future organization** (manual migration when safe):

- `docs/*-airtable-fields.md` → `docs/platform-reference/airtable/`
- `docs/operator-alignment-*.md` → `docs/platform-reference/features/operator-alignment/`
- `docs/scout-*.md` → `docs/platform-reference/features/scout/`
- `docs/gtm-*.md` and `docs/gtm-resources/` → `docs/platform-reference/gtm/`
- `docs/partner-*.md` → `docs/data-intelligence/partner-intelligence/`

Do not move until references are updated and a migration checklist is run.

## Project Memory

- [../AGENTS.md](../AGENTS.md) — concise repo memory for agents
- [../.cursor/rules/deal-capture-implementation-partner.mdc](../.cursor/rules/deal-capture-implementation-partner.mdc) — implementation partner rules
