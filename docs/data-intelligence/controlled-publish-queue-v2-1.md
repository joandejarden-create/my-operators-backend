# Controlled Publish Queue v2.1

**Date:** 2026-07-06  
**Status:** Read-only batch queue  
**Command:** `npm run controlled-publish-queue -- --plan`

> **Authority:** [controlled-platform-field-publishing-v2.md](./controlled-platform-field-publishing-v2.md), [approved-intelligence-field-suggestions-v1.md](./approved-intelligence-field-suggestions-v1.md)

---

## 1. Purpose

Controlled Publish Queue v2.1 is the **batch review layer** for platform field publishing opportunities across priority Brand and Operator intelligence packages.

It answers: *Which entities have low-risk controlled publish candidates? Which are suggested-only? Which destinations are already populated after a controlled publish?*

---

## 2. Relationship to Field Suggestions v1 and Controlled Publish v2

| Layer | Tool | Scope |
|-------|------|-------|
| **Field Suggestions v1** | Per-entity review queue | One operator/brand at a time |
| **Controlled Publish v2** | Single-field guarded write | One fact + one destination |
| **Controlled Publish Queue v2.1** | Batch readiness | All priority tracker entities |

---

## 3. Queue statuses

| Status | Meaning |
|--------|---------|
| **Ready For Controlled Publish** | Low risk, blank allowlisted destination, v2 gates pass |
| **Suggested Only** | Approved mapping; steward review; no blind publish |
| **Blocked** | Unapproved, unsupported, governance block |
| **Needs Steward Review** | Medium/High risk, select validation, identity |
| **No Approved Facts** | No approved fact package |
| **No Destination Mapping** | Fact not mapped to product field |
| **Already Published / Destination Populated** | Live field has value; future changes need review |

---

## 4. Risk levels

- **Low** — blank destination + official source + controlled candidate  
- **Medium** — populated destination, select validation, description updates  
- **High** — identity fields, overwrite risk  

---

## 5. How to use the queue

```bash
npm run controlled-publish-queue -- --plan
npm run controlled-publish-queue -- --entity-type operator --plan
npm run controlled-publish-queue -- --ready-only --plan
```

1. Review **Ready** section → run printed **dry-run** commands only  
2. Review **Already published** after applies (e.g. GHL `specificMarkets`)  
3. **Suggested only** → Mode B steward review; not v2 apply  

---

## 6. Why read-only

No Airtable writes, no apply orchestration, no fact/source approval. Prevents accidental batch overwrites.

---

## 7. GHL post-publish example

After controlled publish of `specificMarkets`:

- `op.markets.regionsSupported` → **Already Published** (live value set)  
- **0** ready controlled candidates  
- Remaining facts → **Needs Steward Review** (Medium/High)  

---

## 8. Hotel Equities example

All approved facts → **Suggested Only** / **Needs Steward Review** — destinations populated; **0** controlled candidates.

---

## 9. Future path

- Persist suggestion status in Airtable (**Approved For Publish**)  
- Steward UI for diff review  
- Batch dry-run orchestration (not apply)  

---

## Reports

`reports/controlled-publish-queue.{md,json}`
