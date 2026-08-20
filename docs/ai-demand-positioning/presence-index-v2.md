# ADP Presence Index V2 + CORE benchmark freeze

Research-only. Live `intentPresenceIndex` and owner UI are unchanged.

- Runner: `npm run adp:presence-index-v2-audit`
- Tests: `npm run test:adp-presence-index-v2`
- Artifact: `reports/ai-demand-positioning/presence-index-v2-core-stability-v1.json`
- Frozen CORE: `lib/ai-demand-positioning/metrics/presence-benchmark-v1.js` (`adp_presence_benchmark_v1`)

V2 grain: observation × territory × provider. All Providers = equal mean of included provider rates, then index. CORE zeros stay in the denominator. No score cap. Production CORE minimum = 4. Adventure remains a 3-peer developing set.
