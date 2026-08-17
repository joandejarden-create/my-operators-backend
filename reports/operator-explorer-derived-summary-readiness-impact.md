# Derived Summary Dependency Check

## Question

Does Airtable-backed Explorer readiness depend on legacy Master fields such as Active Countries, Brands, conversion/resort flags?

## Answer

**No.**

| Classifier | Reads Master derived summaries? |
| ---------- | ------------------------------- |
| Dry-run `buildProfile` | No — uses local Assignments / Presence / Brand Rel arrays |
| Phase 1 `generateAirtablePayloads` | No — uses Airtable Assignments / Presence / Brand Rel record counts |

Legacy Platform `Active Countries` and experience flags were intentionally **not** overwritten in Phase 1 and are **not** readiness inputs.

## Architecture debt

None for readiness. Future list filters must continue to prefer normalized intel tables over stale Master summaries.
