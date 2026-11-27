# HE Contact Intelligence CX V2 — persistence readiness

**Date:** 2026-11-27  
**Airtable writes this cycle:** **none**  
**Classification:** `PERSISTENCE_DESIGNED` (not `PERSISTENCE_VALIDATED` / not `FULL_E2E_VALIDATED`)

## Intent

Eligible verified professional contact facts should land in canonical **HI Organization Contacts** (and shared GDI verified-contact intelligence), not in a second Hotel Explorer contact engine.

## Persist only when all true

| Field | Persist? | Notes |
|---|---|---|
| Person identity (name + stable person key) | Yes | Deduplicate before create |
| Current employer / role | Yes | Evidence-backed |
| Hotel / org association | Yes | Relationship row; do not invent ownership authority |
| Verified business email | Yes | Direct personal/business only; never pattern-guessed |
| Business phone / office extension | Yes | Direct only when evidence supports |
| Public professional profile (LinkedIn URL) | Yes | Public URL |
| Contact channel source | Yes | Provenance |
| Provider verification state | Yes | Explicit enum |
| Last verification date | Yes | |
| Rights / retention classification | Yes | Required before storage |
| Surfe-returned email / phone | **No** | Session-only; `persist_forbidden` |
| Findymail returns | **No** until adapter + terms + spend auth | Currently absent/OFF |
| Generic company channels | Separate org-route rows | Never labeled as direct personal |

## Dedup rules (design)

1. Person key: normalized name + LinkedIn URL when present; else name + org domain.
2. Channel key: person key + channel kind + normalized value.
3. Never create duplicate email/phone rows for the same person key.
4. Multi-section UI appearance does **not** duplicate canonical person records.

## Bethesda status (this cycle)

| Person | Canonical role signal | Direct email/phone persisted | Notes |
|---|---|---|---|
| Katherine Button | VP, Asset Management and Investments (MREC) | No personal direct on file | Company routes only; hotel responsibility not confirmed |
| Patrick Reece | Director, Asset Management (MREC) | No personal direct on file | Same |
| Jeff Mullen | President and CEO (MREC) | No personal direct on file | Same; not a confirmed property GM |

**Missing property leadership (not fabricated):** hotel GM, DOSM / Sales leadership, Revenue leadership.

## Next authorized step (not executed)

1. Dry-run HI Org Contacts upsert plan for any newly verified direct channels.  
2. Founder review of rights/retention.  
3. `--apply` only after standing authorization.  
4. Retrieval proof across restart + HE/Radar shared renderer.
