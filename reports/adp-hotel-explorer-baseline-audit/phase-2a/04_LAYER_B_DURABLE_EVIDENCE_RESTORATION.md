# Phase 2A — Layer B durable evidence restoration assessment

## Status

| Check | Result |
|-------|--------|
| Layer A offline permanence | **PASS** (`notProductionProof: true`) |
| Layer B when not required | Reported `BLOCKED_MISSING_AUTHORITATIVE_EVIDENCE` (informational) |
| Layer B with `ADP_PERMANENCE_REQUIRE_LAYER_B=1` | **EXIT 2** — blocked |
| Required path cited by gate | `data/adp-durable-published/current/adp_hilton_times_square/manifest.json` |
| Local `data/adp-durable-published/current/` | **MISSING** |
| Local `data/adp-durable-published/snapshots/` | Present — Toronto snapshot only (not a `current/` pointer set) |
| Measured-dates export | **MISSING** — `reports/dealality-adp-single-version-publication-contract-v1/admin-published-vs-measured-dates/PRODUCTION_DURABLE_PUBLISHED_VS_MEASURED.json` |
| FS published SoT | Present — 27 properties under `data/ai-demand-positioning/published/` |

## Where authoritative published ADP records live

1. **Durable production volume (Layer B authority):** `data/adp-durable-published/current/<propertyId>/manifest.json` (+ snapshots). Gitignored; expected on Railway `/app/data/adp-durable-published` or a governed local mount.  
2. **Filesystem publication SoT (customer publish packages):** `data/ai-demand-positioning/published/<propertyId>/` — publication ≠ certification.  
3. **Runtime periods:** `data/ai-demand-positioning/runtime/adp_period_*.json`.  
4. **Airtable measurement-history** — live overlay; not Layer B filesystem authority; writes default off.

## What is missing locally

- Entire `current/` durable tree for Hilton (and peers the gate enumerates).  
- Measured-dates production reconciliation JSON.  
- Private certification freeze / `FINAL_31_HOTEL_CERTIFICATION_MATRIX.json` (posture reports absent).

## Repeatable materialization process (read-only; no secrets in Git)

1. On a governed machine with Railway volume or founder-approved durable backup access, mount or copy into **this ADP worktree only**:  
   `data/adp-durable-published/current/` and `snapshots/` as needed.  
2. Optionally export measured-dates audit to the expected reports path (read-only).  
3. Verify Hilton (and Casas / targets) manifests exist:  
   `Test-Path data/adp-durable-published/current/adp_hilton_times_square/manifest.json`  
4. Run:  
   `ADP_PERMANENCE_REQUIRE_LAYER_B=1 npm run test:adp-permanent-full-e2e-certification-v1`  
5. Run: `npm run test:adp-e2e-persistence-contract-v1` and record Layer B status.  
6. Store checksums / period IDs under `reports/adp-hotel-explorer-baseline-audit/phase-2a/evidence/` — **never commit durable package bodies**.  
7. If access is unavailable → remain **BLOCKED**; report required access (Railway volume mount or durable backup export).

## Rules

- Do not invent packages to force Layer B PASS.  
- Do not commit production durable payloads.  
- Do not treat FS `published/` Live manifests as Layer B proof.  
- Do not use synthetic fixtures to claim production durability.

## Precise blocker

**Authoritative durable `current/` manifests are not present in this worktree, and the measured-dates production export is absent.** Phase 2A cannot complete Layer B without founder-authorized read-only access to the durable published volume (or an equivalent governed export).
