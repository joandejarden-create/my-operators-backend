# Executive Intelligence Copy Contract

Binding contract for `Brand AI Intelligence` Executive Summary and Detailed View finding copy.

## Scope

Applies to finding types:

- `LARGEST_COMPETITIVE_GAP`
- `LARGEST_COMPETITIVE_STRENGTH`
- `POTENTIAL_AI_PERCEPTION_GAP`
- `STRONGEST_VALIDATED_ASSOCIATION`
- `PROVIDER_DISAGREEMENT`
- `NARRATIVE_PATTERN`
- `SOURCE_PATTERN`
- `SOURCE_CITATION_GAP`
- `MATERIAL_MOVEMENT`

## Structure

Each finding card uses this structure:

- **Category label** — finding type title (small caps).
- **Executive finding** — self-contained white copy (normally 2–3 concise sentences) that includes:
  - what happened
  - key metric(s)
  - commercial implication
- **Evidence** — strongest 2–3 facts from one coherent evidence construct (substantiation).

**Permanent rule:** Executive findings must communicate both measured result and commercial meaning. Numeric evidence alone is insufficient.
A finding fails editorial validation if white copy merely repeats a category label or generic headline.

## Card layout constraints (UI only)

- Executive finding: normally 4–5 visual lines; max 5 lines (clamp is a ceiling, not a target length)
- Evidence: max 2 visual lines
- Do not use minimum-height padding to force empty space or shorten copy artificially

Detailed View may include one additional supporting context line while preserving the same semantic contract and avoiding duplicate category-title white headers.

## Semantic Rules

### Mentioned vs Cited

- Use `appears`, `is mentioned`, `is represented`, `is associated` for brand presence/representation.
- Use `cited`, `citation`, `cited source` only for URL/domain source evidence.

### Association vs Narrative

- Association = qualifying attribute association observations.
- Narrative = recurring representation across comparable responses/providers/scenarios.
- Do not blend association counts with narrative response coverage in one evidence line.

### Recurrence Language

- `N = 1` → `Early signal`; never `repeated/recurring/consistent`.
- `N = 2` → `Repeated` or `Early repeated evidence`.
- `N >= 3` with qualifying logic → `Recurring` or equivalent recurrence phrasing.

### Denominator Rule

- Use denominators when omission can mislead (`5 of 8 comparable responses`, `3 of 3 observed runs`).
- If denominator is unavailable, do not fabricate.

### Source Causality

Forbidden in executive copy:

- `influences`, `drives`, `causes`, `AI trusts`, `AI learned from`, `AI believes because`

Allowed:

- `sources cited alongside this narrative`
- `owned + external sources cited`

## Evidence Constructs

Each finding must declare one `EVIDENCE_CONSTRUCT`:

- `PRESENCE`
- `GAP`
- `ASSOCIATION`
- `NARRATIVE`
- `TRUTH`
- `PROVIDER_COMPARISON`
- `SOURCE`
- `STABILITY`

Evidence copy must be generated from that construct only.

## Truth / Perception Gaps

When evidence is one explicit response:

- body should say `One observed response ...`
- evidence should carry `Early signal · 1 observation` and governed classification when available.

## Non-redundancy

If Association and Narrative overlap on the same theme:

- prefer Narrative only when it adds incremental validated value (provider breadth, response recurrence, scenario breadth, source linkage, or competitor context).
- otherwise use Association.

**Executive Summary theme diversity (binding):** the selected 3–5 tiles must not repeat a finding type or category title. Cap `LARGEST_COMPETITIVE_GAP` at one tile. Fill remaining slots from other types (competitive strength, perception, association, provider comparison, narrative, source, movement) when evidence exists.

## Copy validation checks

Governance must enforce:

- `BODY_PRESENT`
- `CATEGORY_NOT_REPEATED_IN_WHITE_COPY`
- `NO_DUPLICATE_WHITE_HEADER`
- `EXECUTIVE_FINDING_SELF_CONTAINED`
- `EXECUTIVE_FINDING_INCLUDES_KEY_METRIC`
- `EXECUTIVE_FINDING_INCLUDES_COMMERCIAL_MEANING`
- `WHITE_COPY_4_TO_5_LINE_TARGET`
- `EVIDENCE_SECONDARY_NOT_PRIMARY`
- `NO_HEADLINE_BODY_DUPLICATION`
- `EVIDENCE_PRESENT`
- `EVIDENCE_CONSTRUCT_COHERENT`
- `NO_SEMANTIC_OVERSTATEMENT` / `RECURRENCE_LANGUAGE_SAFE`
- `DENOMINATOR_SAFE`
- `NO_CITATION_MISUSE`
- `NO_CAUSAL_LANGUAGE` (source construct)
