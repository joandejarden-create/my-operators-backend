# Decision & Outcome Migration Audit

**Hotel (Bethesda pilot):** `recLuxvwwxID7U2B8`  
**Script:** `npm run migrate:gdi-decision-outcomes` → `scripts/migrate-gdi-feedback-to-decision-outcomes.mjs`  
**Policy:** Zero invention of timestamps/values; legacy files **not mutated**.

## Existing store disposition

| Source | Disposition | Notes |
|--------|-------------|--------|
| GDI `feedback.json` | **MIGRATE** | Auth hotel validation → canonical Validation (/Action/Outcome when mappable) via `ingestGdiAuthFeedback` |
| GDI `share-validation.json` | **MIGRATE** | Share validations → Validation events only via `ingestGdiShareValidation` |
| ADP founder JSONL (`feedback/action-founder-feedback.jsonl` under monthly-review archive) | **EXTEND / LEAVE separate** | Quality / founder feedback stays on its own path; not collapsed into Decision & Outcome as a replacement |
| GTM Decision Opportunities (Airtable GTM base) | **LEAVE_AS_IS** | Internal Decision Radar; out of scope for this filesystem layer |

## Bethesda apply results

From `reports/decision-outcomes/decision-outcome-migration-audit-apply.json`:

| Metric | Value |
|--------|-------|
| Legacy feedback items | 18 |
| Legacy share items | 0 |
| Migrated feedback | **18** |
| Migrated share | **0** |
| Decisions before | 1 |
| Decisions after | 1 |
| Skipped / ambiguous | none |
| Zero invention | `true` |

**One decision** retained/used: AMWA opportunity `gdi_opp_amwa_2027_annual` (`dec_mu4qcpxz_5394a4c6`). Feedback rows were ingested as events onto that subject — no invented opportunities or fabricated timestamps (provenance stamped `legacyStore`; source `createdAt` used when present).

## Dry-run + apply artifacts

| File | Mode |
|------|------|
| `reports/decision-outcomes/decision-outcome-migration-audit-dry-run.json` | `--dry-run` (wouldMigrate lists; no writes) |
| `reports/decision-outcomes/decision-outcome-migration-audit-apply.json` | `--apply` |

Commands:

```bash
node scripts/migrate-gdi-feedback-to-decision-outcomes.mjs --hotel recLuxvwwxID7U2B8 --dry-run
node scripts/migrate-gdi-feedback-to-decision-outcomes.mjs --hotel recLuxvwwxID7U2B8 --apply
```

## Legacy integrity

- Migration **dual-writes into** `data/decision-outcomes/…` only.
- GDI `feedback.json` / `share-validation.json` are **not rewritten or deleted** by the migrator.
- Ongoing GDI feedback and share APIs continue dual-write for new traffic (see GDI integration report).
