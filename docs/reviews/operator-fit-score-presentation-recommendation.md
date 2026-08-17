# Operator Fit — Score Presentation Recommendation

**Date:** 2026-08-04  
**Underlying numeric engine:** retained · presentation only

---

## Options tested (internal pilot)

| Option | Pattern | Advisor read |
| ------ | ------- | ------------ |
| A | Numeric first `82 / 100` + band | Easy compare; **false precision** risk high |
| B | Band first `Strong Alignment` + smaller `82` | Balanced |
| C | Band + evidence; number in detail only | Highest trust; slightly harder raw compare |

## Assessment

| Criterion | A | B | C |
| --------- | - | - | - |
| Advisor preference (pilot) | Low | Medium | **High** |
| False precision risk | High | Medium | **Low** |
| Ease of comparison | High | Medium | Medium (compare view can still show number) |
| Trust | Lower | Medium | **Higher** |
| Explanatory value | Low alone | Medium | **High** with reasons |
| Decision usefulness | Medium | Medium–High | **High** with shortlist compare |

## Recommendation

**Option C for owner-facing surfaces** (band + reasons/confidence first; numeric in expanded detail / compare).

**Option B acceptable** for internal pilot tooling.

Do **not** ship Option A as the primary owner frame.

Founder approval required before owner pilot UI freeze.
