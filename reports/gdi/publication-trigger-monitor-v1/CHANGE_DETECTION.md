# Change Detection

1. Fetch **known official URLs only** (no SERP on every cycle).
2. Normalize HTML (strip scripts/styles/cookies/copyright noise).
3. Persist `lastContentHash` + material fingerprint (reuses future-watch fingerprinting).
4. Classify: NONE / TRIVIAL / MEANINGFUL / ARTIFACT_PUBLISHED.
5. Meaningful only when: new downloadable artifact, newly appeared publication terms, or material hash change on watched types.
6. Baseline first capture never fires redecomposition.
7. Bounded rediscovery SERP is **opt-in** and only when source unreachable — not used in default cycle.
