# Shadow + Activation + Image Integrity V1 — Final Report

## Answers

### 1. Can V1.1 safely run as recurring read-only shadow monitoring?
**Yes.** Indigo+Kimpton Mexico digest produced with Exact/High gates, dedup state, and **no write path**.

### 2. Which identifiers most improve property matching?
**Official property URL**, **brand property ID / MARSHA / mnemonic**, and **normalized city** (not state labels). See `02-identity-enrichment-proposals.json`.

### 3. Can Medium Pipeline→Open be upgraded via corroboration?
**Conditionally yes** — only with official opening language/banner on the primary property page. Trade press alone **cannot** upgrade to High (`04-opening-corroboration-results.json`).

### 4. Can RE V2 move inactive brands toward activation readiness?
**Yes, as research** — returns readiness % + hard-gate status + recommendation. Never activates. Homepage 403/bot-blocks are **not** treated as discontinuation; census Open/Pipeline and official directory rows corroborate existence.

### 5. Detect brands that should exist in BE because census hotels exist?
**Yes.** Avani (and Tapestry/Spark in this cohort) flagged `brandActivationCandidate: true` when census hotels exist without Active BE.

### 6. Identify exactly why inactive brand is not activation-ready?
**Yes** — scorecard breakdown + `hardGatesFailed` + recommendation rationale per brand in `07-activation-native-results.json`. High % cannot override missing mandatory evidence gates (e.g. Avani 89% → Targeted Remediation, not Ready).

### 7. Image integrity without bad auto-replacements?
**Yes** — classify + propose Keep/Review/Replace Candidate only; no download/rehost.

### 8. Same architecture for retroactive cleanup?
**Yes (design)** — see `10-retroactive-cleanup-design.md` proposal queues + batch sizes. Not executed.

### 9. What remains Webhound-only?
Government/project discovery, opaque ownership/UBO, long-tail unstructured sources, periodic blind external audits, claims without structured official directories, bot-blocked homepage content that still needs human/WH retrieval.

### 10. Top 3 next builds
1. Scheduled read-only shadow cron (Indigo/Kimpton MX → digest file/Slack) — still no writes
2. Steward UI/queue for identity enrichment + activation remediation packs
3. Hilton/Choice full directory extracts + anti-bot fallback for brand homepage existence probes

## Shadow sample

- Hotels checked: 16
- High-confidence: 2
- Review: 0
- Directory gaps: 4
- Stale candidates: 11
- Day-1 suppressed: 0
- Day-2 dedup demo: would suppress 17 / resurface 0
- Runtime: 2913 ms · Cost: $0

## Activation benchmark

- **Avani**: Targeted Remediation Required (89%) · census=2 · activationCandidate=true · hardGates=none
- **Four Points Flex by Sheraton**: Hold — Insufficient Current Evidence (45%) · census=0 · activationCandidate=false · hardGates=brand_exists_unverified,source_authority_official,mexico_or_cala_census_when_claimed
- **Tapestry Collection by Hilton**: Targeted Remediation Required (97%) · census=7 · activationCandidate=true · hardGates=none
- **Spark by Hilton**: Targeted Remediation Required (89%) · census=1 · activationCandidate=true · hardGates=none
- **Radisson Collection**: Hold — Insufficient Current Evidence (37%) · census=0 · activationCandidate=false · hardGates=brand_exists_unverified,source_authority_official,mexico_or_cala_census_when_claimed

## Production posture

Shadow monitoring: **ready (read-only)**  
Activation mode: **experiment / promising** (benchmark only)  
Image integrity: **experiment** (metadata V1)  
Automated writes / activation / image replace: **forbidden**
