# Operator Fit — Development / Asset Differentiation Audit

**Factor:** Asset / Development Experience · weight **20** · `scoreAssetDevelopmentFactor`

## How modes are treated

| Theme | Treatment |
| ----- | --------- |
| New build / conversion / reflag / renovation / turnaround | String overlap into situation lists + comparable hay keywords |
| Mixed-use / residences / F&B / meetings | Mostly **project complexity** factor (12), not this factor |
| Key-count similarity | **Not** implemented in scoring |
| Full-service complexity | Indirect via complexity / differentiators |

## Scoring mechanics

1. Relevant comparables → `min(100, 70 + 10×n)`  
2. Else asset-type overlap ratio → `35 + 50×ratio`  
3. Else development-situation ratio → `30 + 55×ratio`  
4. Breadth without relevance capped ≤40  

## Discrimination limits

| Question | Answer |
| -------- | ------ |
| Repeated vs one example? | Only via relevant **count** (+10 each) |
| Direct conversion vs generic development? | Partial string match — easy to over/under-count |
| Similar scale? | Not explicit |
| Multiple modes collapsed? | **Yes** — one composite score |
| Deal C Santa Fe vs Highgate? | Both **80** despite different comparable geographies/products |

## Verdict

Asset/Development has the most headroom of any factor but currently **buckets** Deal C leaders together. Comparability Strength and geo-of-comparable are underused.
