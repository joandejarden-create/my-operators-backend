# Image Reference Assessment — SerpApi Google Hotels

## Scope
Reference-only integrity analysis. **No download, rehost, or production write.**

## Observations
- Search cards typically include `thumbnail`.
- Property details include `images[]` with `thumbnail` / `original_image` URLs (often Googleusercontent / travel CDN).
- Useful for: property identity corroboration, freshness heuristics, distinguishing rendering vs operating photos (limited without human review).

## Rights
- Image Reuse: **Not Approved**
- Do not persist image URLs into production Census fields pending rights review.
- Benchmark stores at most truncated reference URL lists inside candidate objects for analysis.

## Verdict
Imagery **exists** and can support identity QA. Not a production asset pipeline.
