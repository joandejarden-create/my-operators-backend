# HE-INTEL-001 P1 — Hotel Intelligence Research Engine Foundation

**Contract:** `HOTEL_INTELLIGENCE_RESEARCH_ENGINE_V1` (`1.0.0-p1`)  
**Status:** Implementation complete — awaiting founder review / Integration Controller merge  
**Airtable apply:** disabled  
**Paid providers:** disabled  

## What P1 delivers

Promotes `full-research-workflow-v2` into a versioned, deterministic, resumable engine foundation for the frozen 16-property / 15-HPC pilot.

| # | Capability | Location |
| --- | --- | --- |
| 1 | Versioned E2E contract | `lib/hotel-intelligence/research-engine-v1/contract.js` |
| 2 | Deterministic stage graph | `stages.js` |
| 3 | Canonical HPC + pilot manifest | `pilot-manifest.js` + `fixtures/hotel-explorer/HE-INTEL-001-pilot-16-manifest-frozen-v1.json` |
| 4 | Standardized outputs | `output-schemas.js` |
| 5 | Durable cases / resume | `supervisor.js` → `research-case-store` (reuse) |
| 6 | Failure classifications | `FAILURE_CLASS` in `contract.js` |
| 7 | Org/person/rel/channel dedupe keys | `entity-keys.js` |
| 8 | Provider-neutral interface | `provider-interface.js` |
| 9 | Budget / usage ledger | `budget-ledger.js` |
| 10 | Dry-run vs apply | `apply-policy.js` (APPLY blocked) |
| 11 | Authenticated routes | `api/hotel-intelligence-research-engine-v1.js` + `server.js` mounts |
| 12 | HE Contact VM projection | `he-contact-projection.js` (`ownershipSummary`) |
| 13 | Golden-demo isolation | `golden-demo-isolation.js` (KGPV excluded) |
| 14 | Regression harness | `npm run test:he-intel-001-research-engine-v1` |

## Stage graph (same for every pilot hotel)

```
INIT → CANONICAL_HPC_LOOKUP → IDENTITY_RESOLUTION → OWNERSHIP → ORGANIZATION
→ RELATIONSHIP → PERSON → CONTACT → EVIDENCE_VALIDATION → SYNTHESIS → COMPLETE
```

## Reuse vs new

**Reuse:** `full-research-workflow-v2`, `research-case-store-v2`, `hotel-physical-identity`, contact `view-model` / publication rules, Parallel provider package (interface only in P1), Surfe client (gated off).

**New:** `lib/hotel-intelligence/research-engine-v1/*`, API route module, fixture manifest, foundation test, this doc.

## Explicit non-goals (P1)

- No paid Exa / Parallel / SerpAPI / Brave / Surfe calls  
- No Airtable schema / upsert  
- No claim that the pilot has been researched  
- No expansion beyond frozen 15 HPC IDs  

## P2 / P3 gaps

- **P2:** HI Organizations / Relationships / Persons / Channels / Research Runs Airtable schema + apply path  
- **P3:** Paid research-method cost/value benchmark (see `HE-INTEL-001-research-method-cost-value-benchmark-spec-v1.md`)  

## Test

```bash
npm run test:he-intel-001-research-engine-v1
```
