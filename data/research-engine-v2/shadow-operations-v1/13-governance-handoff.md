# Governance Handoff

```
STEWARD APPROVAL ("Approved for Existing Write Process")
  → EXISTING VALIDATION / WRITE PATH (dry-run first)
  → EXISTING GATES (Company Validated, PVQL, Tab Factory, census, image, freeze)
  → AIRTABLE
```

| Issue type | Path |
|------------|------|
| Pipeline→Open / status | SAFE — Hilton audit / census status scripts |
| Reflag / affiliation | SAFE — census affiliation plans dry-run→apply |
| Parent correction | SAFE — parent census scripts |
| Missing census create | SAFE — directory create plans |
| Identity enrichment | SAFE — property-id / website backfills |
| Brand activation | SAFE — Tab Factory + PVQL + baseline (manual promotion) |
| Image replace | **NO SAFE WRITE PATH YET** — propose only |
| Operator correction | SAFE — OE/census operator links with OE baselines |

Research Engine V2 **never** bypasses these gates.
