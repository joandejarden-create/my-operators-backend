# GDI P1-C — Canonical Full E2E Entry Point

**Contract:** `gdi-canonical-full-e2e-entry-v1`  
**Depends on:** PR #346 (P1-A/B)  
**Paid research:** NONE  
**Publication:** NONE

## Original fragmentation

Multiple paths could run discovery/deep research:

- `npm run gdi:full-e2e` → research-to-page delivery coordinator
- `gdi:full-e2e-fourteen-segment*` → accounting / discovery / original chain
- Hotel-specific campaign scripts

P1-A/B instrumented the discovery matrix, but discovery could finalize as Complete before deep research, and campaign scripts could bypass the shared run identity.

## Canonical entry selected

**Research Full E2E (authoritative for instrumented research):**

```bash
npm run gdi:canonical-full-e2e -- --hotel <id> --mode fixture|dry-run|preflight
```

Module: `lib/group-demand-intelligence/workflow/gdi-canonical-full-e2e-entry-v1.js`

**Delivery / publication Full E2E (unchanged role):**

```bash
npm run gdi:full-e2e -- --hotel <id> --mode full-e2e --dry-run
```

(`scripts/gdi-research-to-page-workflow-v1.mjs`) — still the publication/delivery coordinator; header now points research runs to the canonical entry. Research certification never authorizes publication.

## Changes implemented

1. Shared run lifecycle: parent owns begin → discovery (`executionLifecycle: external`) → deep research accounting → finalize.
2. Discovery-only matrix finalization is always `discoveryPhaseOnly` → PARTIAL (`DISCOVERY_COMPLETED`).
3. Deep-research outcomes recorded into the same PR #336 ledger (`deepResearchStatus` + provider calls).
4. Certification requires deep-research terminal states for Full E2E COMPLETE; `publicationAuthorized: false` always on this certificate.
5. Single-hotel dry/fixture `--run-original-full-e2e-chain` delegates to the canonical entry.
6. Apify remains default `APIFY_NOT_ORCHESTRATED` (P1-B preserved).

## Campaign paths now using it

| Path | Status |
|------|--------|
| `gdi:canonical-full-e2e` | Canonical |
| `gdi:full-e2e-fourteen-segment-chain` (1 hotel, dry, unpaid) | Delegates to canonical |
| Original chain with `recordDeepResearchObservability` | Observability hooks when used |

## Legacy remaining

| Path | Status |
|------|--------|
| Multi-hotel original chain | Legacy with observability hooks; prefer per-hotel canonical |
| Hotel-specific Exa campaign scripts (YOTEL/Radisson/etc.) | Historical; not Full E2E certified |
| `gdi:full-e2e` research-to-page | Delivery coordinator — not a substitute for canonical research ledger |

## Deep-research ledger

Instrumented on the canonical path and when `recordDeepResearchObservability=true` on the original chain. Segments without retained candidates receive explicit SKIPPED deep status (not silent PLANNED).

## Certification

- Discovery-only → PARTIAL / `DISCOVERY_COMPLETED`
- Deep terminal on all planned segments + discovery → may COMPLETE (`DEEP_RESEARCH_COMPLETED`)
- Never implies CUSTOMER_PUBLISHED

## Remaining limitations

- Multi-hotel concurrent chain still legacy (document per-hotel runs).
- research-to-page does not yet auto-invoke canonical research mid-delivery (documented separation).
- Live Apify still not orchestrated (intentional).
- P2 discovery/drawer quality next.
