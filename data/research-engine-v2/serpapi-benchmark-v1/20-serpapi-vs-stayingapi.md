# SerpApi vs StayingAPI (same 25 hotels)

| Metric | StayingAPI | SerpApi Google Hotels |
|--------|------------|------------------------|
| Found | 5/25 | 25/25 (100%) |
| Exact | 0 | 15 |
| High | 3 | 2 |
| Exact+High | 12% | 68% |
| False matches | 0 | 1 |
| Address gaps resolved | ~20% | 53% |
| Coord agreement (≤500m controls) | n/a | 100% |
| Rooms / Keys | NOT_SUPPORTED | NOT_SUPPORTED |
| Cost model | credits / listing | ~1 search / request |

## Materially outperform StayingAPI?
**YES** — identity reliability first; field count secondary.

## Better identity coverage
**SerpApi**

## Better gap-resolution economics
Depends on search delta (7 searches) vs useful Exact/High (17). Cost/useful ≈ 0.41.
