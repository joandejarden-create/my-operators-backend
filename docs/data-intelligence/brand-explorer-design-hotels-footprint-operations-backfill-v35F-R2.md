# Design Hotels Footprint + Operations Backfill v35F-R2

Generated: 2026-07-15T11:46:11.812Z
Mode: **apply**

## Scope
- Target slots: **27**
- Missing before: **24**
- Present before: **3**

## Missing slots (before)
- `footprint.region.mea`
- `footprint.region.apac`
- `footprint.growth_themes`
- `footprint.growth_editorial`
- `footprint.growth_fit`
- `operations.flexibility.design`
- `operations.flexibility.conversion`
- `operations.flexibility.localization`
- `operations.flexibility.operational_rigidity`
- `operations.flexibility.pip`
- `operations.flexibility.prototype`
- `operations.compliance.qa_cadence`
- `operations.compliance.training_rigor`
- `operations.compliance.reporting`
- `operations.compliance.brand_interaction`
- `operations.model.staffing_intensity`
- `operations.model.fb_complexity`
- `operations.model.training`
- `operations.model.reporting_discipline`
- `operations.model.qa_rhythm`
- `operations.model.technology`
- `operations.operator_compat.summary`
- `operations.operator_compat.fit`
- `operations.operator_compat.tags`

## Rows created
- `footprint.region.mea` (Footprint & Growth)
- `footprint.region.apac` (Footprint & Growth)
- `footprint.growth_themes` (Footprint & Growth)
- `footprint.growth_editorial` (Footprint & Growth)
- `footprint.growth_fit` (Footprint & Growth)
- `operations.flexibility.design` (Operating Model)
- `operations.flexibility.conversion` (Operating Model)
- `operations.flexibility.localization` (Operating Model)
- `operations.flexibility.operational_rigidity` (Operating Model)
- `operations.flexibility.pip` (Operating Model)
- `operations.flexibility.prototype` (Operating Model)
- `operations.compliance.qa_cadence` (Operating Model)
- `operations.compliance.training_rigor` (Operating Model)
- `operations.compliance.reporting` (Operating Model)
- `operations.compliance.brand_interaction` (Operating Model)
- `operations.model.staffing_intensity` (Operating Model)
- `operations.model.fb_complexity` (Operating Model)
- `operations.model.training` (Operating Model)
- `operations.model.reporting_discipline` (Operating Model)
- `operations.model.qa_rhythm` (Operating Model)
- `operations.model.technology` (Operating Model)
- `operations.operator_compat.summary` (Operating Model)
- `operations.operator_compat.fit` (Operating Model)
- `operations.operator_compat.tags` (Operating Model)

## Rows patched
- `footprint.region.am` (`recSPPM6AZUbogqj3`) — v35F-R2 normalize region card format
- `footprint.region.cala` (`rec1A6N8CZoKq05CG`) — v35F-R2 normalize region card format
- `footprint.region.eu` (`reciOPHSoTc5Ti98x`) — v35F-R2 normalize region card format

## Founder review queue
- (none)

## Census companion (run separately before or after apply)
```bash
npm run apply-design-hotels-census-footprint-fix -- --dry-run
npm run apply-design-hotels-census-footprint-fix
```

## Guardrails
- Company Validated untouched: **yes**
- Active profile approval: **not set**
- External owner ready (projected): **yes**

## Apply command
```bash
npm run brand-explorer-design-hotels-footprint-operations-backfill -- --brand design-hotels --apply --approve-brand-explorer-v35F-R2-design-hotels-footprint-operations-backfill --confirm-no-company-validation-claim --confirm-no-active-profile-approval --confirm-no-summary-url-field --confirm-source-traceability-preserved --confirm-no-visible-source-urls-in-owner-copy --confirm-affiliation-not-franchise-language --confirm-design-hotels-only --confirm-cala-census-footprint-companion-run
```
