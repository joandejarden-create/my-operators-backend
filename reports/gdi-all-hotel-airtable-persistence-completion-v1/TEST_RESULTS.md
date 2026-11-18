# Test results

## Unit tests

```bash
npm run test:gdi-all-hotel-airtable-persistence-completion-v1
```

**Result: PASS**

Covers:

- Research vs GDO layer separation (`commerciallyQualified` promotion)
- Schema gate blocked (`REQUIRES_FOUNDER_SCHEMA_APPROVAL`)
- Bulk apply / schema create fail-closed
- YOTEL legacy ID non-recreation
- Existing GDO preservation without re-qualification overwrite
- Org-name conflict quarantine
- Local universe discovery (YOTEL 42 baseline)

## Dry-run inventory

```bash
npm run gdi:all-hotel-airtable-persistence-completion-v1
```

**Result: OK** (read-only Airtable GDO lists + FS inventory)

| Metric | Count |
|--------|------:|
| Hotels | 19 |
| Local research opportunities | 659 |
| Live Airtable GDO rows (sum) | 674 |
| Research inventory pending schema | 659 |
| ID overlap (FS ∩ AT) | 511 |
| GDO preserve / unchanged | 509 |
| GDO CREATE candidates | 0 |
| Commercially qualified FS (enum+contact) | 55 |
| Conflicts quarantined | 2 |
| Airtable-only GDOs | 163 |
| YOTEL legacy recreate blocks | 0 |

## CI precheck

```bash
npm run dealality:ci-precheck
```

**Result: PASS** (gitleaks cache mode; workflow concurrency present)

Note: local working tree still holds unrelated preserved research WIP (Full E2E state dirs, etc.). This PR stages only the persistence-completion package paths.
