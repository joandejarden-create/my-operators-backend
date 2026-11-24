# Founder review and publication gate verification

Lifecycle enforced by this PR:

`RESEARCH` → `VERIFIED/QUALIFIED` → `FINALIZED STAGED` → `FOUNDER REVIEW` → `CURRENT PUBLISHED`

| Gate | Status |
|------|--------|
| Research ≠ local presentation | **PASS** — Research tab removed; admin escape only |
| FQO required for new STAGED candidacy | **PASS** — quality gate opt-in flag documented for staging scripts |
| STAGED activation automatic? | **NO** — no pointer writes in this PR |
| CURRENT activation automatic? | **NO** |
| Founder hold/reject binding on FQO? | **PASS** — `FOUNDER_HELD` / `FOUNDER_REJECTED` fail closed |
| Sales Ready required for local finalized? | **NO** |
| Airtable / deploy / paid research | **Not performed** |
