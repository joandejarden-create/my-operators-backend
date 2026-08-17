# Approved Fact Correction v1

**Date:** 2026-07-06  
**Status:** Steward-reviewed PI fact correction (dry-run default)  
**Command:** `npm run approved-fact-correction`

> **Authority:** [partner-intelligence-profile-governance-runbook.md](./partner-intelligence-profile-governance-runbook.md), [approved-intelligence-platform-field-publishing-v1.md](./approved-intelligence-platform-field-publishing-v1.md)

---

## 1. Purpose

Approved Fact Correction v1 is the **controlled process for correcting an approved Partner Intelligence fact value** after steward review — without damaging provenance, audit history, governance, or platform-published fields.

It closes the loop when a **product field** has already been corrected (e.g. GHL `specificMarkets`) but the underlying **approved PI fact** still carries an older steward-approved value.

---

## 2. Value layers

| Layer | Field / location | Mutable by v1? |
|-------|------------------|----------------|
| **Extracted Value** | PI Extracted Facts | **No** — preserved as extraction provenance |
| **Approved Value** | PI Extracted Facts | **Yes** — steward correction target |
| **Corrected approved value** | Same as Approved Value after apply | Result of correction |
| **Product-published value** | Operator Setup / Brand Setup | **No** — use [controlled-platform-field-publishing v2](./controlled-platform-field-publishing-v2.md) correction mode |

---

## 3. Why product correction does not update the fact

Platform field correction writes **only** the allowlisted Setup column. PI facts are a separate audit layer:

- Original extraction (`Extracted Value`) stays intact.
- Prior `Approved Value` reflects steward approval at approval time.
- Product field may be corrected to a cleaner steward interpretation without retroactively changing PI until an explicit **fact correction** is approved.

This preserves provenance and avoids silent drift between extraction history and steward decisions.

---

## 4. Safety gates

1. Fact exists  
2. Human Review Status ∈ **Approved**, **Edited**  
3. Corrected value non-empty  
4. Corrected value ≠ current display value (`Approved Value` or fallback `Extracted Value`)  
5. Reason provided  
6. Not Rejected / Pending / Quarantined / Superseded / Invalid / Do Not Use  
7. Not an identity field (`op.snapshot.companyName`, etc.) unless explicitly allowed  
8. **Extracted Value not modified**  
9. Sources not modified  
10. Platform fields not modified  
11. Governance not modified  
12. Company Validated not modified  
13. Apply requires `--apply` + `--approve-approved-fact-correction`

**Patch allowlist only:** `Approved Value`, `Human Review Status`, `Reviewer Notes`, `Last Updated`

---

## 5. GHL example

| Item | Value |
|------|-------|
| Fact | `reccszsLnWjA5fPnp` — `op.markets.regionsSupported` |
| Current Approved Value | `Latin America, Colombia, Peru, Chile, Guatemala` |
| Product field (already corrected) | `Colombia, Chile, Guatemala, Peru` |
| Evidence | `reckrUB2WmnSm02g3` — [GHL destinations](https://www.ghlhoteles.com/en/destinations/) |
| Corrected fact value | `Colombia, Chile, Guatemala, Peru` |

**Dry-run:**

```bash
npm run approved-fact-correction -- --fact-id reccszsLnWjA5fPnp --correct-value "Colombia, Chile, Guatemala, Peru" --reason "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context." --evidence-source-id reckrUB2WmnSm02g3 --dry-run
```

---

## 6. Rollback

Restore from report:

- `Approved Value` → `rollback.previousApprovedValue`
- `Human Review Status` → prior status (usually `Approved`)
- `Reviewer Notes` → prior notes (remove appended correction line if reverting manually)

Extracted Value unchanged throughout.

---

## 7. Effect on field suggestions

After fact correction apply:

- `approved-intelligence-field-suggestions` uses `Approved Value` (or `Extracted Value` fallback) as proposed value.
- When fact matches corrected product field, suggestion risk may drop (aligned live vs proposed).
- Populated destinations remain **suggested-only** / steward review — not blind controlled publish.

Re-run audit, suggestions, and [controlled-publish-queue](./controlled-publish-queue-v2-1.md) **after** fact correction apply.

---

## 8. Effect on governance / publish readiness

- Fact correction does **not** publish governance or change Company Validated.
- Approved/Edited fact counts for governance readiness remain valid.
- `Human Review Status` → **Edited** signals steward-adjusted value (existing convention for manual changes).

---

## 9. Future: Fact Corrections table

**v1:** Append-only `Reviewer Notes` + `Approved Value` update on existing Extracted Facts row.

**Future option:** `Partner Intelligence - Fact Corrections` table with correction ID, prior value, new value, steward, evidence source link, and apply timestamp — without replacing extraction history.

---

## Reports

```bash
reports/approved-fact-correction-<factId>.{md,json}
reports/approved-fact-correction.{md,json}   # latest alias
```

## Change Impact

| Tier | **High** — writes PI Approved Value on apply |
| Rollback | Restore prior Approved Value + Reviewer Notes from report |

## Regression checklist

- [ ] `npm run test:approved-fact-correction`
- [ ] GHL dry-run shows Extracted Value preserved
- [ ] Apply without approval token fails
- [ ] Pending/Rejected facts blocked
- [ ] No platform/governance writes in patch
