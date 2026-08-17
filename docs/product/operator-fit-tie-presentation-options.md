# Operator Fit — Tie Presentation Options

**Audit only.**

## Problem

Stable `candidateId` tie-break is correct for deterministic computing but inappropriate as an **owner-facing claim that #1 beat #2** when Displayed delta = 0 (Deal C).

## Options

| Option | Behavior | Deal C effect | Pros | Cons |
| ------ | -------- | ------------- | ---- | ---- |
| **A** Continue #1/#2 via ID | Current | Santa Fe #1, Highgate #2 | Stable | **Implies false separation** |
| **B** Tied ordinal `#1` / `#1` | Shared rank | Both #1 | Honest | Owners may still ask “who’s better?” |
| **C** No ordinals below materiality | “Leading Candidates” list | Grouped equals | Clear | Needs threshold |
| **D** Rank bands (Leading / Competitive / Potential) | Both Leading-or-Validation tier | Banded | Matches Option D candidate-set | Needs copy design |

## Material separation (distribution-supported)

From compression audit: many pairs within 1–3 points.

**Recommended rule for future UI (not implemented):**

- Δ Displayed **&lt; 1.0** → treat as **tie** (no ordinal advantage)  
- Δ **&lt; 2.0** → “effectively tied / minor separation” — suppress hard #1 claim  
- Δ **≥ 3.0** → ordinal ranks allowed  

Supported by near-tie density and Deal C exact tie; not purely aesthetic.

## Recommendation

**Option C (or B)** for owner view when Δ&lt;1, combined with candidate-set tiering. Keep ID tie-break **internal only** for sort stability.
