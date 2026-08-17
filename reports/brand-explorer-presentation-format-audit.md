# Brand Explorer Presentation — format consistency audit

Generated: 2026-07-23T12:07:45.409Z
Sources: Airtable, fixtures (5374 rows)
Rows: 11942 · Brands: 124 · Slot keys: 202

## Summary

- **137** slot keys with **2+ answer formats** across brands (0 high-priority, 45 medium).
- **High-priority** usually means documented canonical levels (e.g. flexibility indicators) mixed with narratives or compound phrases like `Moderate to High`.

### Format type legend

| Type | Meaning |
|------|---------|
| `canonical_level` | Single label: Minimal, Low, Moderate, Medium, High, Very high |
| `numeric_scale` | `1`–`6` or `4/6` |
| `compound_level` | e.g. Moderate to High, Low–Moderate |
| `narrative` | Long prose or multiple sentences |
| `short_line` / `medium_line` | Brief copy |
| `structured_labels` | standards.requirement labeled lines |

## All slot keys with mixed formats

| Slot key | Formats | Expected rule |
|----------|---------|---------------|
| `commercial.lever.corporate_group` | medium_line, multi_paragraph, narrative | — |
| `commercial.lever.data_analytics` | narrative, medium_line, multi_paragraph | — |
| `commercial.lever.digital_marketing` | narrative, multi_paragraph, medium_line | — |
| `commercial.lever.international` | multi_paragraph, narrative, medium_line | — |
| `commercial.lever.leisure_destination` | multi_paragraph, narrative, medium_line | — |
| `commercial.lever.reputation_qa` | multi_paragraph, narrative, medium_line | — |
| `commercial.lever.revenue_management` | narrative, multi_paragraph, medium_line | — |
| `commercial.lever.sales_catering` | multi_paragraph, narrative, medium_line | — |
| `footprint.openings` | multi_paragraph, narrative, medium_line | — |
| `footprint.portfolio_mix.archived` | medium_line, short_line, narrative | — |
| `footprint.region.eu` | narrative, multi_paragraph, medium_line | region_card |
| `hero.benefit_zones` | narrative, multi_paragraph, medium_line | — |
| `hero.operator_compat` | narrative, medium_line, multi_paragraph | — |
| `insight.similar` | short_line, medium_line, narrative | — |
| `loyalty.elite` | narrative, medium_line, short_line | — |
| `loyalty.hero_title` | short_line, medium_line, narrative, multi_paragraph | — |
| `loyalty.kpi.mix` | short_line, medium_line, narrative | — |
| `loyalty.owner_lens` | medium_line, narrative, multi_paragraph | — |
| `loyalty.proof` | narrative, medium_line, short_line | — |
| `materials.caseStudy` | narrative, multi_paragraph, short_line, medium_line | — |
| `materials.file` | medium_line, short_line, narrative, url_only, multi_paragraph | — |
| `materials.gallery.1` | medium_line, narrative, short_line | — |
| `materials.gallery.2` | medium_line, narrative, short_line | — |
| `materials.gallery.3` | narrative, medium_line, short_line | — |
| `materials.gallery.4` | narrative, medium_line, short_line | — |
| `materials.gallery.5` | short_line, narrative, medium_line | — |
| `materials.gallery.6` | medium_line, narrative, short_line | — |
| `operations.compliance.training_rigor` | short_line, narrative, medium_line | short_line |
| `operations.model.fb_complexity` | medium_line, narrative, short_line | short_line |
| `operations.model.primary_model` | narrative, short_line, medium_line | short_line |
| `operations.model.qa_rhythm` | short_line, narrative, medium_line | short_line |
| `operations.model.staffing_intensity` | short_line, narrative, medium_line | short_line |
| `operations.model.systems_integration` | narrative, short_line, medium_line | short_line |
| `operations.model.training` | narrative, short_line, medium_line | short_line |
| `operations.operator_compat.tags` | mixed_phrase, line_list, comma_list | tag_list |
| `overview.bestAt.2` | narrative, medium_line, short_line | card_copy |
| `overview.bestAt.3` | medium_line, narrative, short_line | card_copy |
| `overview.development_model` | medium_line, narrative, multi_paragraph | — |
| `overview.differentiators.commercial` | narrative, multi_paragraph, medium_line | — |
| `overview.differentiators.identity` | narrative, multi_paragraph, medium_line | — |
| `overview.relative_positioning` | narrative, medium_line, multi_paragraph | — |
| `overview.typical_use_case` | narrative, medium_line, multi_paragraph | — |
| `standards.last_reviewed` | medium_line, short_line, narrative, multi_paragraph | — |
| `standards.requirement` | structured_labels, narrative, unstructured_text | standards_structured |
| `valueOwners.lifecycle.3` | short_line, narrative, medium_line | card_copy |
| `commercial.differentiator` | medium_line, narrative | — |
| `commercial.intro` | narrative, multi_paragraph | — |
| `commercial.kpi.lens` | short_line, medium_line | — |
| `commercial.lever.distribution` | multi_paragraph, narrative | — |
| `commercial.theme` | medium_line, narrative | — |
| `economics.cash.preopening` | multi_paragraph, narrative | — |
| `economics.cash.ramp` | multi_paragraph, narrative | — |
| `economics.cash.renewal` | narrative, multi_paragraph | — |
| `economics.cash.steadystate` | multi_paragraph, narrative | — |
| `economics.fee` | medium_line, narrative | — |
| `economics.fee.change` | narrative, medium_line | — |
| `economics.incentives` | medium_line, narrative | — |
| `economics.intro` | medium_line, narrative | — |
| `economics.kpi.fee_stack` | narrative, medium_line | fee_or_term_line |
| `economics.kpi.incentives` | medium_line, narrative | fee_or_term_line |
| `economics.kpi.performance` | medium_line, narrative | fee_or_term_line |
| `economics.legal` | medium_line, narrative | — |
| `economics.lifecycle.renewal` | medium_line, short_line | — |
| `economics.negotiability` | narrative, medium_line | — |
| `economics.opening.financials` | narrative, multi_paragraph | — |
| `economics.opening.process` | narrative, multi_paragraph | — |
| `economics.opening.step.1` | narrative, medium_line | — |
| `economics.opening.step.2` | medium_line, narrative | — |
| `economics.opening.step.3` | medium_line, narrative | — |
| `economics.opening.step.4` | medium_line, narrative | — |
| `economics.opening.step.5` | narrative, medium_line | — |
| `economics.performance_exit` | narrative, multi_paragraph | — |
| `economics.risk` | medium_line, narrative | — |
| `economics.support_burden` | narrative, medium_line | — |
| `economics.term_renewal` | narrative, medium_line | — |
| `footprint.editorial` | narrative, medium_line | — |
| `footprint.editorial_bullets` | narrative, medium_line | — |
| `footprint.geo_intro` | narrative, multi_paragraph | — |
| `footprint.geo.summary` | medium_line, narrative | — |
| `footprint.growth_editorial` | narrative, medium_line | — |
| `footprint.growth_fit` | narrative, medium_line | — |
| `footprint.growth_themes` | medium_line, narrative | — |
| `footprint.growth.narrative` | narrative, medium_line | — |
| `footprint.momentum` | multi_paragraph, short_block | momentum_row |
| `footprint.momentum_label` | medium_line, short_line | — |
| `footprint.portfolio_mix` | narrative, title_body_pair | portfolio_mix |
| `footprint.region.1` | narrative, medium_line | region_card |
| `footprint.region.2` | narrative, medium_line | region_card |
| `footprint.region.3` | medium_line, narrative | region_card |
| `footprint.region.am` | narrative, multi_paragraph | region_card |
| `footprint.region.apac` | narrative, multi_paragraph | region_card |
| `footprint.region.cala` | narrative, multi_paragraph | region_card |
| `footprint.region.mea` | narrative, multi_paragraph | region_card |
| `insight.summary` | narrative, multi_paragraph | — |
| `loyalty.ecosystem` | narrative, multi_paragraph | — |
| `loyalty.implications.ops` | narrative, medium_line | — |
| `loyalty.implications.pnl` | narrative, medium_line | — |
| `loyalty.implications.systems` | narrative, medium_line | — |
| `loyalty.kpi.hotels` | short_line, medium_line | — |
| `loyalty.kpi.members` | short_line, medium_line | — |
| `operations.compliance.brand_interaction` | narrative, medium_line | short_line |
| `operations.compliance.qa_cadence` | narrative, medium_line | short_line |
| `operations.compliance.reporting` | narrative, medium_line | short_line |
| `operations.model.brand_involvement` | narrative, medium_line | short_line |
| `operations.model.management_option` | medium_line, narrative | short_line |
| `operations.model.pre_opening` | narrative, medium_line | short_line |
| `operations.model.reporting_discipline` | medium_line, narrative | short_line |
| `operations.model.technology` | narrative, medium_line | short_line |
| `operations.model.typical_ownership` | narrative, medium_line | short_line |
| `operations.operator_compat.summary` | narrative, medium_line | — |
| `operations.standards_philosophy` | multi_paragraph, narrative | — |
| `overview.bestAt.1` | medium_line, narrative | card_copy |
| `overview.owner_experience` | narrative, multi_paragraph | — |
| `overview.proof_operator` | medium_line, narrative | — |
| `overview.proof.1` | narrative, medium_line | — |
| `overview.proof.2` | medium_line, narrative | — |
| `overview.proof.3` | narrative, medium_line | — |
| `overview.proof.4` | narrative, medium_line | — |
| `overview.proof.5` | narrative, medium_line | — |
| `overview.proof.6` | medium_line, narrative | — |
| `overview.scenario.1` | narrative, medium_line | card_copy |
| `overview.scenario.2` | narrative, medium_line | card_copy |
| `overview.scenario.3` | narrative, medium_line | card_copy |
| `overview.scenarios` | narrative, multi_paragraph | — |
| `overview.why_value` | narrative, multi_paragraph | — |
| `valueOwners.lifecycle.1` | narrative, medium_line | card_copy |
| `valueOwners.lifecycle.2` | medium_line, narrative | card_copy |
| `valueOwners.lifecycle.4` | medium_line, narrative | card_copy |
| `valueOwners.lifecycle.5` | narrative, medium_line | card_copy |
| `valueOwners.lifecycle.6` | narrative, medium_line | card_copy |
| `valueOwners.overview` | multi_paragraph, narrative | — |
| `valueOwners.owner_considerations.1` | medium_line, narrative | — |
| `valueOwners.owner_considerations.2` | medium_line, narrative | — |
| `valueOwners.questions.1` | medium_line, narrative | — |
| `valueOwners.questions.3` | narrative, medium_line | — |
| `valueOwners.scenarios` | narrative, multi_paragraph | — |
| `valueOwners.watchouts` | narrative, multi_paragraph | — |

## Recommended normalization

1. **`operations.flexibility.*`** — Use **one canonical word** per row (`High`, `Moderate`, …) or **`1`–`6`**. Avoid `Moderate to High` and long narratives on these six slots; put prose in `operations.standards_philosophy` or conversion copy slots.
2. **Choice full-bundle templates** — Many `*-full.json` fixtures use narrative bodies for flexibility; align with Radisson Blu / docs example (`High`, `Very high`, etc.) when pushing to Airtable.
3. **`operations.flexibility.operational_rigidity`** — Often `Moderate to High` in CHI bundles vs single word elsewhere; pick one convention.
4. Run this script after bulk applies: `node scripts/audit-brand-explorer-presentation-formats.mjs`
