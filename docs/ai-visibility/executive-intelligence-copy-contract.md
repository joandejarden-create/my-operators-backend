# Executive Intelligence Copy Contract

Binding contract for `Brand AI Intelligence` Executive Summary and Detailed View finding copy.

## Scope

Applies to finding types:

- `LARGEST_COMPETITIVE_GAP`
- `POTENTIAL_AI_PERCEPTION_GAP`
- `STRONGEST_VALIDATED_ASSOCIATION`
- `PROVIDER_DISAGREEMENT`
- `NARRATIVE_PATTERN`
- `SOURCE_PATTERN`
- `SOURCE_CITATION_GAP`
- `MATERIAL_MOVEMENT`

## Structure

Each finding card uses four layers:

- **Category label** — finding type title (small caps).
- **Headline** — factual statement of what is happening (finding).
- **Body** — commercial interpretation (normally two concise sentences):
  - sentence 1 = measured result in plain language
  - sentence 2 = why it matters to a hotel brand executive
- **Evidence** — strongest 2–3 facts from one coherent evidence construct (substantiation).

**Permanent rule:** Executive findings must communicate both measured result and commercial meaning. Numeric evidence alone is insufficient.

A finding fails editorial validation if **Body** merely repeats **Headline**.

## Card layout constraints (UI only)

- Headline: max 2–3 visual lines
- Body: normally 3–5 visual lines; max 5 lines (clamp is a ceiling, not a target length)
- Evidence: max 2 visual lines
- Do not use minimum-height padding to force empty space or shorten copy artificially

Detailed View may use a slightly richer body (one additional supporting sentence or context line) while preserving the same semantic contract.

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

## Copy validation checks

Governance must enforce:

- `BODY_PRESENT`
- `BODY_NOT_HEADLINE_RESTATEMENT`
- `BODY_ADDS_COMMERCIAL_INTERPRETATION`
- `EVIDENCE_PRESENT`
- `EVIDENCE_CONSTRUCT_COHERENT`
- `NO_SEMANTIC_OVERSTATEMENT` / `RECURRENCE_LANGUAGE_SAFE`
- `DENOMINATOR_SAFE`
- `NO_CITATION_MISUSE`
- `NO_CAUSAL_LANGUAGE` (source construct)
