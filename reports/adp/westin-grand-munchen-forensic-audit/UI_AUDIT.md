# UI Audit

UI symptoms (subject-only Overall, blank displacement, blank top alternative, 8 attributes) are **faithful renders** of the published payload.

| Binding | Diagnosis |
|---------|-----------|
| AI Competitive Set | Reads competitiveRankingByTerritory / competitiveSet — subject-only was correct for empty bind |
| Competitive Displacement | Reads lostDemand.displacement — empty array |
| Top Observed AI Alternative | Reads competitiveSet.topObservedAlternative — null |
| Attributes | Reads realityGap — 8 tracked |

**No hotel-specific UI fix.** Fix data/bind → UI heals.
