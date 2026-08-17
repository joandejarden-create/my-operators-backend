# D.4D Profile Actual Completion — Founder Review (Profile ONLY)

## Does every retained Profile field have an actual researched answer for every Production operator?

**Yes — for retained Core Product fields.** Actual research coverage **100%** (468/468 cells). Unresolved controlled placeholders **0**.

Platform & Markets **not started** — authorization required before D.4D Platform pass.

---

| Item | Result |
| ---- | ------ |
| Previous mistake | D.4C counted controlled placeholders as "response completeness" |
| New KPI | **Actual research coverage** separate from unresolved/blank |
| Retained Profile fields | 13 columns + Master Parent/OM/MA |
| Before actual / unresolved | 438 / 30 (6.4% unresolved placeholders) |
| After actual / unresolved | **468 / 0** |
| Research performed | 16 operators with targeted field-specific research |
| Writes applied | 16 profile patches |
| Deprecated columns | 54 fields → hide in **LEGACY** view; exclude from Core Product grid |
| Fit | **BLOCKED** |

## Key fixes (examples)

- **AADESA / Álvarez Argüelles / Tremun / Sonesta / GSF / Cenote** — `companyHistory` from official/about/filings (not "Not publicly disclosed")
- **Accor / Hyatt** — `companyDescription` restored to factual corporate descriptions
- **Meliá, Four Seasons, Mandarin, Rosewood, Hyatt, Auberge, AADESA, Álvarez, Tremun, Driftwood** — `differentiators` replaced placeholder abstention with researched company-specific facts
- **Luxury operators** — `additionalExperience` populated (Urban/Resort) from portfolio evidence
- **Argentina operators** — `propertyTypes` / Soft Brand filled from official portfolio context

## Manual Airtable actions required

1. Create view **`D.4D Core Product`** with only retained fields (see `reports/operator-setup-core-clean-view-recipe.md`)
2. Create view **`LEGACY — Deprecate Hide`** with the 54 deprecated columns for archival reference only
3. **Do not** use full 68-column grid for founder review

## Approvals needed before Platform D.4D

- [ ] Accept Profile actual completion at 100% retained coverage
- [ ] Accept deprecated-field hide/LEGACY view plan
- [ ] Authorize Platform & Markets field-by-field pass (same actual-research standard)

Backup: `backups/operator-setup/d4d-profile/2026-08-11T09-42-49/`
