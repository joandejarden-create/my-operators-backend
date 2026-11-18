# Phase 2A — Repeatable ADP hotel execution contract

**Implementation authority:** `lib/ai-demand-positioning/orchestration/adp-autonomous-hotel-full-e2e-v1.js`  
**Goal:** One consistent process for any eligible hotel. Hotel-specific scripts are thin wrappers only.  
**CLI gap:** No single `package.json` alias yet for the orchestrator (Phase 2B item) — invoke via documented node entry / OS workflow until aliased.

## Eligibility

- Property ID in `CANONICAL_ADP_31_PROPERTY_IDS` **or** explicit founder expansion auth.  
- Resolved identity (HPC `rec…` / Dealality hotel id / census link) with collision checks.  
- Bethesda remains in scope even when `COMPATIBILITY_ONLY` / internal-only visibility policy.

## Required inputs

| Input | Source |
|-------|--------|
| `adpPropertyId` | Canonical ID |
| Census / HPC identity | Ledger + census-links fixture + live HPC when authorized |
| Scenario / prompt versions | Scenario registry + frozen eligibility |
| Peer set intent | Brand portfolio governance |
| Authorization ledger | Paid supervisor handoff + founder scopes |
| Prior periods (if refresh) | Runtime + published manifests |

## Process (idempotent stages)

1. **S0 Identity** — resolve; stop on ambiguity.  
2. **S1 Research** — live property research (non-paid default).  
3. **S2 Persist identity** — only with write approval.  
4. **S3 Attributes** — Gate 1 dedup before monitoring.  
5. **S4 Peers** — freeze / approve peer membership.  
6. **S5 Cost auth** — hard gate.  
7. **S6–S7 Monitoring** — paid Core / Loyalty only when authorized; capture original responses + hashes.  
8. **S8 Analytics** — competitor resolution, displacement, RG; Gate 2.  
9. **S8 Reconciliation** — permanent measurement reconciliation; fail closed.  
10. **S9 Stage report** — owner payload / executive surfaces.  
11. **S9B History** — only with history write flags + allowlist.  
12. **S10–S11** — PDF/share test pins + customer release audit (Gate 3).  
13. **S12 Founder readiness** — publication **not** implied.  
14. **Publication** — separate explicit authorization.  
15. **Recurring measurement** — REFRESH mode + comparability rules.

## Provenance & evidence

- Record model/provider, prompt/scenario versions, period IDs, response hashes.  
- Prefer original response authority over re-derived mentions.  
- Evidence packs must be non-empty and client-readable for customer surfaces.  
- Deduplicate safe reruns via run manifests under `data/ai-demand-positioning/autonomous-full-e2e-runs/`.

## Separation rules

| Mode | May claim |
|------|-----------|
| Synthetic / offline fixtures (Layer A) | Code contract PASS only |
| Durable production evidence (Layer B) | Production durability when manifests present |
| Customer publication | Only after founder publication auth + surface gates |

**Offline test PASS must never auto-approve customer-facing results.**

## Failure recovery

- Stage results recorded on manifest; resume from last completed stage.  
- Max safe retries bounded in orchestrator (`MAX_SAFE_RETRIES`).  
- Paid stages never retry into spend without re-auth.  
- Missing durable evidence → **BLOCKED**, not FAIL-as-PASS.

## Certification vs publication

- Certification lifecycle ≠ Live publishStatus.  
- `ADP_REQUIRE_CERTIFICATION_BEFORE_PUBLISH=1` remains the commercial posture target for next official periods (Phase 2B execution).
