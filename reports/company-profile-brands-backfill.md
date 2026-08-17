# Company Profile — Brands You Operate / Support backfill

**Date:** 2026-06-04  
**Field:** `Brands You Operate / Support` → linked **Brand Setup - Brand Basics** record IDs only.

## Policy (no hallucination)

Brands are linked **only** when a verified source already exists in Airtable:

| Source | What it uses |
|--------|----------------|
| **Existing link field** | Current `rec…` IDs on the company row |
| **Lookup names** | `Brand Name (from Brands You Operate / Support)` — exact match to Brand Basics **Brand Name** only |
| **3rd Party Operator - Basics** | `Brands Managed` linked records, matched by company name |
| **Operator Setup - Profile & Positioning** | Brand link field on profile row, matched by company name (parentheticals like `(CALA)` stripped) |
| **Brand Basics Parent Company** | **Hotel Brands (Franchise)** only — all brands whose **Parent Company** matches the company name (legal suffixes stripped, e.g. `Inc.`) |

**Not used:** guessing from company name, web search, `# of Brand` counts, or fuzzy brand name matching. Ambiguous duplicate Brand Name rows are **skipped**.

## Commands

```bash
# Audit (no writes)
node scripts/audit-company-profile-brands.mjs
node scripts/audit-company-profile-brands.mjs --json=reports/company-profile-brands-audit.json

# Dry-run
node scripts/backfill-company-profile-brands.mjs

# Write to Airtable
node scripts/backfill-company-profile-brands.mjs --apply

# Single company
node scripts/backfill-company-profile-brands.mjs --apply --id recXXXXXXXX
```

## Code

- `lib/company-profile-brands-backfill.js` — inference + patch builder
- `scripts/audit-company-profile-brands.mjs`
- `scripts/backfill-company-profile-brands.mjs`
- `scripts/test-company-profile-brands-backfill.mjs`

## Latest audit snapshot (your base)

- **78** Company Profile rows
- **253** Brand Basics brands
- **26** companies with at least one verified brand after merge
- **52** still empty — no operator brand links, no franchisor parent match in Brand Basics, and no resolvable lookup names

Empty franchisors (e.g. Four Seasons, Meliá, Barceló) usually mean those brands are **not yet in Brand Setup - Brand Basics** with a matching **Parent Company**, not that the script can invent them.

## Manual follow-up

1. Add missing brands to **Brand Setup - Brand Basics** (with correct **Parent Company** for franchisors).
2. Complete **Operator Setup - Profile & Positioning** / **3rd Party Operator - Basics** `Brands Managed` links for management companies.
3. Re-run audit, then `--apply`.

Hide the read-only lookup column **Brand Name (from Brands You Operate / Support)** in Airtable grid views if redundant; keep the link field as the single write target.
