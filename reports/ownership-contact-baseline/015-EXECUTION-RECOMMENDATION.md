# 015 — Execution Recommendation

## Scale strategy

**OWNER_PORTFOLIO_FIRST** (with country playbooks as supporting work).

## Single next packet

**OWNERSHIP_CONTACT_25_HOTEL_END_TO_END_CANARY**

Do not run another broad audit.

## Why not remediation-only first?

Architecture is PARTIAL but not blocking a bounded canary: HOTEL_TO_OWNER, owner-target-resolver, CI store, and research handoff exist. The gap is populated evidence + contact paths. A 25-hotel canary will force persistence of failure stages and prove the chain.

## Hard stops observed

- No ownership-v3 corpus in this checkout
- Contact store empty
- Airtable ownership writes disabled by design
- Explorer demo dependency
