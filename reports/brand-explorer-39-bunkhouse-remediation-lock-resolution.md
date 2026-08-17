# Bunkhouse remediation_locked — protected 39 resolution

- Generated: 2026-07-27T16:38:26.002Z
- Classification: **A_stale_report_or_transient_resolver_state**
- Live display: **active_profile_ready** · shouldRenderFullProfile=**true**
- Image uniqueness: **true** · role-match: **true**
- Airtable writes: **false**

## Root cause

Prior PVQL captured bunkhouse as draft_applied_with_defects / remediation_locked while live Basics + Presentation now resolve to active_profile_ready with uniqueness and role-match PASS. No Bunkhouse writes required.

## Prior PVQL snapshot

- cohort: `restored_legacy_public` · publicFull=true · state=`active_profile_ready`

## Fresh PVQL confirmation

- Ran: `npm run test:brand-explorer-public-visibility-quality-lock -- --public-full-only`
- Result: **publicFull=39** · **allPass=true** · bunkhouse cohort=`restored_legacy_public` · `remediationLocked=false`
- Strict 39 baseline: **PASS** (`test:brand-explorer-39-active-public-full-baseline`)
