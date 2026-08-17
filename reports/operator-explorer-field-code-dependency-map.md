# Operator Explorer — Field Code Dependency Map

**Date:** 2026-08-09  
**Mode:** Code search across mappings, scripts, Fit, Explorer UI  
**Rule:** Absence of one grep hit ≠ safe to remove; check aliases/maps/bindings.

Central maps inspected:

- `api/lib/third-party-operator-new-two-field-bindings.json`
- `api/lib/operator-*-map.js`
- `api/lib/partner-intelligence-field-map.js`
- `lib/operator-fit/*`
- `lib/operator-intelligence/*`
- `lib/partner-intelligence/operator-explorer-*`
- `public/js/operator-explorer*.js`

---

## Dependency classifications (key fields)

| Field | Readers | Writers | Validators | Legacy Usage | Safe to Change? |
| ----- | ------- | ------- | ---------- | ------------ | --------------- |
| Master.`company_name` | Explorer, Fit, setup, APIs | Setup writers, research onboarding | Identity gates | — | **No** (identity) |
| Master.`submission_status` | Active universe loaders, Fit eligibility, Explorer filters | Setup, promote scripts, Argentina onboarding | Fit eligibility | Draft/Approved legacy values coexist | **No** — taxonomy critical |
| Master.`Validation Status` / `Usage Permission` / `Company Validated` | Explorer trust / PI governance | PI / research (sparse) | Governance | Parallel to Brand Validation | Careful normalize only |
| Master.`External Display Status` | Explorer display | Sparse | Display gates | Brand parallel | Careful |
| Master links to Claims / Market Presence / Shortlist | Fit geo, shortlist | Intel apply scripts | Post-write validate | New (2026-08) | Keep |
| Platform.`Active Countries` | Fit geo, Explorer markets | Calibration / wave populate | Taxonomy validation | Duplicates Market Presence concept | **No delete**; migrate to derive |
| Platform.`Active Markets / Cities` | Fit geo soft | Sparse | — | Soft | KEEP + NORMALIZE |
| Platform.`Market Presence Type` (flat multi) | Fit geo tiers (legacy path) | Sparse | — | **Superseded** by Market Presence table | DEPRECATE LATER for scoring |
| Profile.`chainScalesSupported` | Fit scale, Explorer | Setup | High coverage | — | KEEP |
| Profile.`Service Models Supported` | Fit / Explorer | Setup | — | — | KEEP + NORMALIZE |
| Profile.`brands` (link Brand Basics) | Fit brand factor, Explorer | Setup | — | — | KEEP |
| Profile.`Brand Families Operated` | Display / soft fit | Setup | Soft taxonomy | Duplicates brands link | KEEP + NORMALIZE / eventual DERIVE |
| Commercial.`Management Structures Supported` | Fit structure | Calibration populate | Critical | — | KEEP |
| Commercial.`Conversion / Reflag Experience` | Fit (intended) | Almost empty | — | Prefer assignment-derived | KEEP + future DERIVE |
| Commercial.`New-Build Opening Experience` | Fit extras | Sparse | — | Prefer assignment-derived | KEEP + future DERIVE |
| Commercial.`bf_*` fields | Explorer cards, soft fit | Setup | — | Marketing-ish | DEPRECATE LATER for scoring weight |
| Governance.`Offered Services` | Fit services | Sparse | — | Table-stakes risk | KEEP; deprioritize scoring |
| Governance RM/Sales/F&B selects | Fit extras / Explorer | Sparse | — | Generic if presence-scored | KEEP; do not over-weight |
| Case Studies.* | Explorer narrative | Setup / seed scripts | Not numeric OAS | `situation` option pollution | KEEP; MOVE comps to Assignments later |
| Case Studies.`Why Comparable` / `Comparability Strength` | Fit comparable path (intended) | Intel schema apply | — | New | KEEP |
| Brand Relationships presentation rows | Explorer Brand tab | Setup writer | Section keys | Not approval graph | KEEP for Explorer UI |
| Claims.* | Intel overlay, Fit evidence | Calibration apply | Publication policy | 28 rows | KEEP; extend selects |
| Market Presence.* | Fit geo eligibility | Wave migration apply | Presence taxonomy | 42 rows | KEEP; extend geo grain |
| Shortlist.* | Internal pilot | Shortlist store | — | Workflow | KEEP; never Explorer master |
| ODR alignment score fields | Outreach snapshot | Match score server | — | Not shortlist | KEEP workflow |
| Company Profile.* | Platform onboarding | Company profile API | — | Parallel company concept | Do not merge blindly |
| Companies.* | Outreach CRM | Outreach | — | Legacy outreach | Do not use for Explorer |

---

## Bulk field classes (886 fields)

| Class | Approx scope | Safe to rename/remove now? |
| ----- | ------------ | -------------------------- |
| Active read — Master/Profile/Platform/Commercial/Governance scoring fields | ~40 critical | **No** |
| Active write — Operator Setup bindings (417 form fields path) | Large commercial/profile surface | **No** without binding audit |
| Research dependency — Claims, Market Presence, calibration overlays | Growing | **No** |
| Explorer presentation children (Leadership/Brand Rel/Materials/…) | Row stores | Rename risky; section keys coded |
| Derived/report-only | Completeness audits, readiness reports | Soft |
| Legacy `bf_*` / card blobs / DNA | Still read by Explorer | DEPRECATE LATER only |
| No dependency found (many Platform/Commercial sparse fields) | Hundreds sparsely used | Still **not** safe to delete without per-field binding search |

---

## Search method for future change proposals

1. Exact Airtable field name  
2. Binding JSON keys / `map_*` constants  
3. Prefill / form keys  
4. Fit factor config  
5. Fixtures JSON keys  
6. Reports referencing the field  

**Verdict:** No operator-relevant field is recommended for immediate removal in this phase.
