# Brand Explorer Phase 2F — Root-cause report (founder visual QA)

**Verdict:** Phase 2E automated A/B/C = 1.0 is **not** founder-approved production quality.  
**Curio / Ascend:** not founder-approved. No scale population. No Airtable writes.

## Observed founder failures → root causes

| Founder finding | Root cause | Fix locus |
|---|---|---|
| Paragraphs in title fields | Generator/repair stamped slot keys + diligence prose into `title`, or reused long strings as headings | `be-phase2f-presentation-repair-v1` short_title enforcement + visual gate `paragraph_in_title` |
| Missing card / scenario imagery | Scenario cards never assigned `imageUrl`; gallery logos counted as images; Phase 2E population gates treated text-only as pass | Assign official evidence imagery; reject logos/placeholders |
| Insufficient / excessive section depth | Single generic narrative reused; no field-specific min/max from template | Content presentation contract with per-semantic-type limits |
| Repeated economics text | Quality repair prefixed the same FDD total-investment paragraph onto every `economics.*` slot | Metric-specific economics presentation bodies |
| Empty list / weak cards | Collection items allowed thin or empty bodies under population-only gates | `empty_list_item` + cardinality gates |
| Hero / mapping issues | Overlay + render aliases passed structural gates without visual geometry checks | Visual gates + browser validation |
| Comparison misalignment | Independent full-page scroll; collection match by array order | Section-locked + Changes-only modes; title/signature matching |

## Why A/B/C hit 1.0

Phase 2E gates rewarded **presence of text** and **render substring matches**, with aliases that could pass `actual_rendering` without owner-useful differentiation. Near-duplicate economics and missing scenario images were not blocking.

## Permanent regression baseline

- Contract: `configs/brand-explorer-content-presentation-contract-v1.json`
- Gates: `lib/brand-explorer/be-phase2f-visual-quality-gates-v1.js`
- Inventory: `reports/brand-explorer/phase2f-visual-quality/01-curio-baseline-field-inventory.json`
- Defective fixtures fail; repaired candidates must improve visual pass rate

## Safety

Airtable writes: **0** · Paid research: **0** · Publication: **none** · Mass population: **none**
