# HIF V7 — Next CURRENT_BACKWARD Discovery Cycle (Track A)

**Publication:** CUSTOMER_BLOCKED  
**Spend posture:** Free public research only; $0 metered without founder approval  
**Goal:** Grow verified first-100 (principal BCT/MCT + accessible engagement) from **0** after V7 pilot revalidation

## Priority source classes (yield over volume)

1. Active hotel **management** RFPs / QMA replacements
2. Hotel **operator replacement** processes (convention / public / institutional owners)
3. Brand / franchise **selection** for operating or late-stage hotels
4. Franchise **nonrenewal** / brand-exit notices with owner decision window
5. Owner RFPs for **management proposals** (private + public)
6. Portfolio / multi-asset management tenders
7. **Concession** RFPs for existing hotels (track as HOTEL_CONCESSION_OPERATING — adjacent)
8. Credible prospective **conversion** processes (existing hotel → new brand) — principal BCT only when evidenced

## Country allocation (priority, not ceiling)

| Country | Focus | Native language |
|---------|-------|-----------------|
| US | State parks concessions, convention authorities, urban renewal hotel RFPs, municipal/port authority QMAs | EN |
| MX | FONATUR / state tourism / municipal hotel operation and brand tenders | ES |
| BR | Public hotel concessions, Embraetur-adjacent, municipal PPPs | PT |
| GB | Local authority hotel operator tenders, NHS/estate lodging, Crown/estate concessions | EN |
| CA | Municipal / provincial hotel ops + redevelopment with explicit management ask | EN/FR |

Worldwide eligibility remains uncapped; these five are research intensity priorities.

## Modules to reuse (do not duplicate pipelines)

- `lib/hotel-intelligence/sec-edgar/hif-v7-opportunity-status-v7.js`
- `lib/hotel-intelligence/sec-edgar/hif-v7-procurement-engagement-v7.js` *(new)*
- `lib/hotel-intelligence/sec-edgar/commercial-opportunity-recovery.js`
- `lib/hotel-intelligence/sec-edgar/timing-geo-actionability.js` (+ freshness / gap / first-100)
- Scripts: `hif-v7-current-backward-commercial-pilot.mjs`, `hif-commercial-opportunity-recovery.mjs`
- SEC name-free continuous discovery continues **independently** (do not merge yield gates)

## Finite batch design

| Checkpoint | Batch size | Cost | Stop if |
|------------|------------|------|---------|
| CB-1 | 15–25 current RFP/tender URLs (US+CA first) | $0 | 0 open principal BCT/MCT after triage |
| CB-2 | Expand MX/BR/GB native sources | $0 | Yield &lt; 1 verified open / 20 sources |
| CB-3 | Corroborate shortlist → dossiers | $0 | Status CLOSED / AWARDED |

Every candidate must fill WHERE / WHEN / HOW CURRENT / HOW TO ENGAGE before Priority A.

## Test gates before PR

```bash
npm run test:hif-v7-opportunity-status
npm run test:hif-v7-procurement-engagement   # add with this cycle
npm run test:hif-commercial-opportunity-recovery
npm run test:hif-timing-geo-actionability
npm run dealality:ci-precheck
```

## Integration Controller handoff

1. Work only in `ws-hif-v6-track-a` (or HE home if Track A retired) on short-lived `feat/hif-v7-…` from latest `origin/main`
2. Do **not** merge from product chat — open PR → `AWAITING_INTEGRATION_CONTROLLER`
3. IC sequence after stack `#528→#529→#531→#532` lands: merge next HIF PR only when mandatory CI + gitleaks green
4. Post-merge: `npm run dealality:post-merge-sync -- --mode post-merge --stream he --pr <n>` then `--apply` when safe; preserve dirty HE/ADP/GDI/BE/Track A/B
5. Never deploy / publish / paid enrich

## Explicit research gaps (Country × Timing)

- MX / BR / GB: near-zero CURRENT_BACKWARD coverage vs US
- US: underweight private-owner management RFP boards vs public concessions
- Franchise nonrenewal windows: almost no CURRENT_BACKWARD hits yet
- SEC historical expirations remain EXPIRATION_ONLY_PROXY — research only when they unlock a live decision

## Expected source tests (CB-1)

- NYS Parks concessions board (pattern validated by Gideon)
- Additional MPEA / convention authority “current bids”
- Colorado / Texas / Florida urban renewal + airport hotel ops RFPs
- Ontario / BC municipal hotel ops (post-Kenricia filter: require management/ops language)
- UK Contracts Finder / Find a Tender hotel operator keywords
