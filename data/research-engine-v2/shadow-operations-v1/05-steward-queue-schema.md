# Steward Queue Schema

Statuses: New | Review | Approved for Existing Write Process | Rejected | Needs More Research | Deferred | Resolved | Superseded

**Approved for Existing Write Process ≠ Airtable write** — it only unlocks the existing dry-run → gate → apply path.

Priorities: P0 Critical Integrity · P1 High · P2 Medium · P3 Low  
Low confidence alone cannot be P0/P1.

See `lib/research-engine-v2/steward-queue.js` (`createQueueItem`).
