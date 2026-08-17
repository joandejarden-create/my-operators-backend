# Controlled Platform Field Publishing v2

**Date:** 2026-07-06  
**Status:** Guarded single-field write path (operator only)  
**Command:** `npm run controlled-platform-field-publishing`

> **Authority:** [approved-intelligence-field-suggestions-v1.md](./approved-intelligence-field-suggestions-v1.md), [approved-intelligence-platform-field-publishing-v1.md](./approved-intelligence-platform-field-publishing-v1.md)

---

## 1. Purpose

Controlled Platform Field Publishing v2 is the **first guarded write path** from approved Partner Intelligence facts into **platform-facing Operator Setup fields**.

It writes **one allowlisted field** per run, only after **live re-validation** of all safety gates and an explicit approval token.

---

## 2. Suggestions vs controlled publish

| Layer | Tool | Writes? |
|-------|------|---------|
| **Suggestions v1** | `approved-intelligence-field-suggestions` | No — review queue |
| **Controlled publish v2** | `controlled-platform-field-publishing` | Yes — **one field**, allowlist only |

---

## 3. Required safety gates (all must pass)

1. Entity type supported (`operator` in v2)  
2. Destination table + field allowlisted  
3. Source fact **Approved** or **Edited**  
4. Source **Approved for Explorer Use = Yes**  
5. Profile governance allows display  
6. Classification = **Controlled Publish Candidate**  
7. Risk level = **Low**  
8. Destination live value **blank**  
9. Not a governance field  
10. Not Company Validated / Company Validation Date  
11. Not an identity field (unless explicitly allowlisted — none in v2)  
12. Not scoring / deal-fit field  
13. `--apply` present (for writes)  
14. `--approve-controlled-field-publish` present (for writes)  
15. `--fact-id` or `--suggestion-key` present  

**Default:** dry-run (no write).

---

## 4. Allowed destination fields (v2)

| CLI `--destination-field` | Table | Column | Fact key |
|---------------------------|-------|--------|----------|
| `specificMarkets` | Operator Setup - Platform & Markets | `specificMarkets` | `op.markets.regionsSupported` |

---

## 5. Blocked destination fields

- All governance columns (Validation Status, Company Validated, etc.)  
- Identity fields (`company_name`, brand name, etc.)  
- `multipleSelects` / `singleSelect` without v2 allowlist  
- Scoring / deal-fit keys (`op.dealFit.*`, `op.meta.*`)  
- Brand Setup tables (v2 operator-only)  
- Any field not in `V2_ALLOWED_OPERATOR_DESTINATIONS`  

---

## 6. GHL first publish example

**Entity:** GHL Hoteles (GHL Holding) — `reciI2tYQBfMoMK9G`  
**Fact:** `reccszsLnWjA5fPnp` — `op.markets.regionsSupported`  
**Proposed:** `Latin America, Colombia, Peru, Chile, Guatemala`  
**Destination:** Platform & Markets → `specificMarkets` (blank live value)  
**Risk:** Low  

**Dry-run:**

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --fact-id reccszsLnWjA5fPnp --destination-field specificMarkets --dry-run
```

**Apply (steward only — after dry-run review):**

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --fact-id reccszsLnWjA5fPnp --destination-field specificMarkets --apply --approve-controlled-field-publish
```

---

## 7. Rollback / audit logging

Each run writes:

- `reports/controlled-platform-field-publishing-<recId>.{md,json}`  
- Latest alias reports  

Report includes:

- previous value, new value, destination record ID  
- source fact ID, source ID, fact key  
- timestamp, mode (dry-run / apply)  
- rollback note (restore previous value manually)  

v2 does **not** auto-modify Partner Intelligence facts or governance.

---

## 9. Steward correction path (populated destinations)

Controlled publish v2 **correctly blocks second writes** once a destination is populated (`destination_not_blank`). Corrections to populated allowlisted fields must use **correction mode** — not another blind controlled publish.

| Rule | Correction mode |
|------|-----------------|
| Destination state | Must be **populated** |
| `--correct-value` | Required — steward-approved target value |
| `--reason` | Required — audit trail |
| Default | Dry-run |
| Apply | `--apply` + `--approve-controlled-field-correction` |
| Writes | Allowlisted destination field only |
| Untouched | Governance, Company Validated, PI facts, sources, scoring |

### specificMarkets value shape

`specificMarkets` should prefer **country/market names** (e.g. `Colombia, Chile, Guatemala, Peru`). Broader regional framing such as **Latin America** should remain in PI evidence and fact context unless the destination field is explicitly defined as regional.

### GHL correction example (2026-07-06)

After first controlled publish, live value was `Latin America, Colombia, Peru, Chile, Guatemala`. Official destinations page (`reckrUB2WmnSm02g3`) lists four countries only. Recommended correction: `Colombia, Chile, Guatemala, Peru`.

**Steward plan report:**

```bash
npm run controlled-platform-field-publishing-correction
```

**Correction dry-run:**

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --destination-field specificMarkets --correct-value "Colombia, Chile, Guatemala, Peru" --reason "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context." --dry-run
```

**Apply (founder approval only):**

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --destination-field specificMarkets --correct-value "Colombia, Chile, Guatemala, Peru" --reason "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context." --apply --approve-controlled-field-correction
```

Reports: `reports/controlled-platform-field-publishing-correction-reciI2tYQBfMoMK9G.{md,json}`

**Fact alignment:** Product correction does not update PI facts. Use [approved-fact-correction-v1.md](./approved-fact-correction-v1.md) to align `Approved Value` on the source fact.

---

## 10. Future Field Suggestions table

v2 uses live suggestion rebuild (audit → suggestions) per run.

**Future:** persist suggestions in Airtable with status **Approved For Publish**; controlled publish would require matching row status.

---

## Change Impact

| Tier | **High** — first product field write path (single-field allowlist) |
| Rollback | Restore `previousValue` on Platform & Markets row from report |

## Regression checklist

- [ ] `npm run test:controlled-platform-field-publishing`
- [ ] `npm run test:controlled-publish-queue`
- [ ] `npm run controlled-publish-queue -- --plan`
- [ ] GHL post-apply: `specificMarkets` shows as already published in queue (0 ready)
- [ ] Apply without approval token fails
- [ ] HE populated destination blocked
- [ ] No governance writes

**Batch queue:** [controlled-publish-queue-v2-1.md](./controlled-publish-queue-v2-1.md) — `npm run controlled-publish-queue -- --plan`
