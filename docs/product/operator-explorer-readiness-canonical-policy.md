# Operator Explorer — Canonical Readiness Policy

**Status:** Recommended (founder approval required to adopt in code)
**Separates:** Research Complete Enough · Explorer Publishable · Strong Explorer Profile  
**Not:** Operator Fit Data Readiness (diagnostic only)

## Principles

1. Same intelligence ⇒ same readiness class, whether evaluated from local dry-run or Airtable.
2. Prefer **named Assignments** SoT; aggregate/representative rows do not count.
3. Prefer distinct **countries** and distinct **brand names**, not raw row counts alone.
4. **Record Purpose** gates **Explorer Publishable**, not Research Complete Enough.
5. Do not lower content gates merely to recover dry-run headline counts.

## Record Purpose semantics

| Value | Meaning | Explorer Publishable? |
| ----- | ------- | --------------------- |
| Production | Real entity in persistent product universe | Eligible if content gates pass |
| Research | Real entity; research / graduation incomplete | **Not** Explorer Publishable (may be Research Complete Enough) |
| Test Fixture | Synthetic / demo | Never |

> Should Research ever be Explorer Publishable? **No** under this policy — graduate to Production first.

## Lifecycle vs Record Purpose

| Field | Responsibility |
| ----- | -------------- |
| Record Purpose | Universe membership (Production / Research / Test Fixture) |
| submission_status (lifecycle) | Workflow state (Draft / In Review / Research Stage / Active / …) |

Do **not** create a third status field. Avoid confusing combos where possible (see audit matrix), but Purpose wins for publishable universe.

## Research Complete Enough

Internal intelligence usable when **any** of:

- ≥1 named Assignment, or
- ≥1 Market Presence country, or
- ≥1 typed Brand Relationship

## Explorer Publishable

All of:

1. Record Purpose = **Production**
2. Named Assignments ≥ **2**
3. Distinct presence/assignment countries ≥ **1**
4. Track 2 only: ≥1 **Brand Managed Capability** relationship (or document exception)
5. Not Test Fixture

## Strong Explorer Profile

Explorer Publishable **and**:

1. Named Assignments ≥ **5**
2. Distinct countries ≥ **2**
3. Distinct brand names (from Brand Relationships and/or Assignments) ≥ **2**

## Explicit non-gates

- Legacy Master Active Countries / experience flags (derive later; do not require for readiness)
- Claim category free text
- City / Assignment Count on Presence (optional enrichment)
- Fit scores

## Implementation note

Replace Phase 1 apply thresholds (`asg≥5/8`, `mp≥2/3`) with this policy in one shared module (e.g. `lib/operator-explorer/readiness.js`) used by dry-run builders and Airtable payload generators.
