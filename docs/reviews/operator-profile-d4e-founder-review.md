# D.4E Visible Profile Field Completion — Founder Review

## Did we stop redefining completeness and finish the live fields?

**Yes for the six visible problem fields** — live-verified **36/36** each. Platform and Fit remain **blocked**.

| Field | Decision | Live coverage |
| ----- | -------- | ------------- |
| yearEstablished | POPULATE (operating-origin year) | **36/36** |
| yearsInBusiness | DERIVE `2026 − yearEstablished` | **36/36** (Grupo Marta YIB corrected 65→66) |
| brands | DERIVE BR ∪ Current Assignments → Brand Basics | **36/36** (created missing Auberge / Barceló / Meliá / Gran Meliá / ME by Meliá Brand Basics) |
| primaryServiceModel | **KEEP — POPULATE** | **36/36** |
| managementPhilosophy | **KEEP — POPULATE** | **36/36** (Accor generic rewritten) |
| missionStatement | **KEEP — POPULATE** | **36/36** (Playa generic rewritten) |

## Active Profile set after D.4E

19 business-data columns (+ Operator link + Master Parent/OM/MA) — all populated for Production.

## Still physical but REMOVE from active product

**48** legacy columns (tagline, overview_*, brand_*_json, ESG, locationType*, etc.) still exist in Airtable with blanks.

**Manual action:** move them to view `LEGACY — Deprecate Hide`. Do not leave them in the founder working grid.

Recipe: `reports/operator-setup-core-clean-view-recipe.md` → **D.4E Core Product**

## Approvals before Platform

- [ ] Accept six-field 36/36 live completion
- [ ] Accept KEEP for primaryServiceModel / managementPhilosophy / missionStatement
- [ ] Confirm LEGACY view hides the 48 REMOVE columns
- [ ] Authorize Platform field-by-field pass

Backup: `backups/operator-setup/d4e-profile/2026-08-11T13-40-47/`  
Preview: `docs/reviews/operator-profile-d4e-final-live-preview.md`
