# Executive Intelligence Copy Contract

Binding contract for `Brand AI Intelligence` Executive Summary finding copy.

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

- **Headline**: factual statement of what is happening.
- **Body**: usually two concise sentences:
  - sentence 1 = what is happening
  - sentence 2 = why it matters commercially
- **Evidence**: strongest 2–3 facts from one coherent evidence construct.

Card layout constraints (UI only):

- body clamped to 5 lines
- evidence clamped to 2 lines

Line clamps are visual guards and must not trigger filler copy.

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

