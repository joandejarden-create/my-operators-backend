# Assignment Schema Stress Test

Assignments researched (dry-run): **84** across 27 entities.

| Field | Populated % | Band | Recommendation |
| ----- | ----------: | ---- | -------------- |
| propertyName | 100% | Frequently populated | KEEP essential/high-value |
| country | 58.3% | Often populated | KEEP essential/high-value |
| city | 57.1% | Often populated | KEEP essential/high-value |
| brand | 100% | Frequently populated | KEEP essential/high-value |
| brandParent | 32.1% | Occasionally populated | KEEP optional |
| keys | 3.6% | Rarely populated | KEEP optional / do not require |
| chainScale | 28.6% | Occasionally populated | KEEP optional |
| urbanOrResort | 51.2% | Often populated | KEEP essential/high-value |
| developmentContext | 100% | Frequently populated | KEEP essential/high-value |
| operatingStructure | 100% | Frequently populated | KEEP essential/high-value |
| assignmentStatus | 100% | Frequently populated | KEEP essential/high-value |
| allInclusive | 3.6% | Rarely populated | KEEP optional / do not require |
| brandedResidences | 2.4% | Rarely populated | KEEP optional / do not require |
| lastVerified | 58.3% | Often populated | KEEP essential/high-value |

## Keep for v1 Create
propertyName, Operator, country, assignmentStatus, brand (link), developmentContext, operatingStructure, urbanOrResort, source/evidence, lastVerified, current/historical

## Optional
city, keys, chainScale, brandParent, allInclusive, brandedResidences, start/end dates

## Do not require at create
mixedUse, meetingsConvention, fbComplexity, state/province (add when needed)

## Missing recurring
Named official property URL field; Census hotel link; clearer Assignment Status for “pipeline managed but not open”

## Taxonomy issues
Development Context mapping from Case Study situation is noisy — keep Assignment taxonomy separate from Case Study situation select.
