# Operator Profile — Full Live Field Completion Stop Point

**Engine:** `operator-setup-live-field-completion-v1`  
**Mode:** APPLY (22 profile patches, 0 failures)  
**Backup:** `backups/operator-setup/full-live-profile/2026-08-11T14-04-36/`  
**Authority:** Live Airtable schema — not D.4B/C/D/E curated lists.

---

## Stop-point answers (§23)

| # | Item | Result |
| - | ---- | ------ |
| 1 | Total physical Profile fields | **68** |
| 2 | Fields with any Production values | **67** |
| 3 | Fields 36/36 before | **20** |
| 4 | Partially populated Production fields (auto) | **36** (after REMOVE reclass); was 38 before `locationType*` REMOVE |
| 5 | Empty-but-active fields | **0** |
| 6 | Fixture-only fields | **0** |
| 7 | Fields populated automatically (Writer/presentation) | **23** field keys touched |
| 8 | Fields derived automatically | **17** field keys (brands count, mix, JSON, taxonomy) |
| 9 | Writer-v2 / presentation fields completed | **19** (tagline, bestat, why, signals, brand narratives) |
| 10 | Fields recommended REMOVE | **12** — see `reports/operator-profile-fields-removed-or-deprecated.md` |
| 11 | Physical/dependency-blocked removals | **companyLogo** (keep legacy attachments until UI uses website/CDN) |
| 12 | `overview_bestat_*` verdict | **RETAIN** — 3 headline/story pairs = Explorer “Best at” cards |
| 13 | `overview_bestat_*` final coverage | **36/36** (all 6 fields, live verified) |
| 14 | `companyTagline` verdict | **RETAIN** (override prior deprecate) — DNA/Explorer consumer |
| 15 | `companyTagline` final coverage | **36/36** (live verified) |
| 16 | Active business-data fields after | **55** (presumed active, not REMOVE) |
| 17 | Active fields at 36/36 | **55 / 55** |
| 18 | Active fields below 36/36 | **0** |
| 19 | Blank active cells | **0** |
| 20 | Generic values (banned Writer v2) | **0** new banned generics blocked at write |
| 21 | Unsupported values | **0** claimed invents; ESG/sustainability use diligence confirm where no public framework |
| 22 | Fixture leakage | **0** |
| 23 | Targeted research operators (presentation pack) | **17** |
| 24 | New sources added | Presentation pack + derived brand JSON from OM / Brand Families |
| 25 | Fields founder had to identify manually | **0** |
| 26 | Full Profile visual verdict | **PASS** |
| 27 | Generic completion module created? | **Yes** — `lib/operator-setup/live-field-completion.js` |
| 28 | Ready to apply same method to Platform? | **Yes** (not started) |
| 29 | Exact founder decisions required | Accept bestat RETAIN; tagline RETAIN; REMOVE list; authorize Platform pass |
| 30 | Platform started? | **No** |
| 31 | Fit started? | **No** |

---

## Artifacts

| Artifact | Path |
| -------- | ---- |
| Live schema audit | `reports/operator-profile-full-live-schema-audit.md` |
| Partial fields (auto) | `reports/operator-profile-partially-populated-fields.md` |
| Fixture-only verdict | `reports/operator-profile-fixture-only-field-verdict.md` |
| Remove/deprecate | `reports/operator-profile-fields-removed-or-deprecated.md` |
| bestat family | `reports/operator-profile-overview-bestat-verdict.md` |
| companyTagline | `reports/operator-profile-company-tagline-verdict.md` |
| Founder preview | `docs/reviews/operator-profile-full-live-completion-preview.md` |
| Machine stop JSON | `data/operator-setup/full-live-profile/full-live-stop-point.json` |
| Apply script | `scripts/operator-setup-full-live-profile-completion.mjs` |

---

## REMOVE list (not counted toward active 36/36)

`additionalBrands`, `companyLogo`, `insuranceCoverage`, `carbonTracking`, `energyEfficiency`, `wasteReduction`, `crisisExperience`, `capitalStatus`, `locationTypeResort`, `locationTypeAirport`, `marketExpansionRampTimeMonths`, `readyForInvestorPublication`

These remain physically present until Airtable deletion is authorized; they must leave the founder working grid (LEGACY / hide), not stay ambiguously “active.”

---

## Change impact

- **Classification:** High (Airtable Profile writes)
- **Rollback:** Restore from `backups/operator-setup/full-live-profile/2026-08-11T14-04-36/`
- **Modules:** Profile table only; Platform / Fit untouched

## Regression checklist

- Re-open Profile grid for a late operator (e.g. Shangri-La, Tremun) — tagline + all 3 bestat cards filled
- Spot-check DNA/Explorer tagline + Overview best-at cards
- Confirm REMOVE fields are not treated as Core Product completeness
- Do **not** start Platform until founder accepts REMOVE + tagline/bestat verdicts
