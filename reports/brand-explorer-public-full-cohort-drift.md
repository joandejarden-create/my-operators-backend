# Public-Full Cohort Drift

Generated: 2026-07-22T23:04:20.719Z

## Verdict

Observed 14 = 11 intentional public-full (7 primary + 4 visibility-restored) + 3 legacy-seed built-blocked brands (country, suburban, woodspring) unlocked via legacyVisibilityUnlock — not via fullyReady and not via explicit public restore. quality-inn + radisson/blu/red remain non-public-full.

- Expected baseline: **11**
- Expected after explicit restore of 7: **18**
- Observed: **14**
- fullyReady auto-public bug: **false** — shouldRenderFullProfile uses FULL_PROFILE_DISPLAY_STATES or legacyVisibilityUnlock(historicalApproved && rows && visuals && imageUniqueness). fullyReady/tab-factory pass is not an unlock input.

| Brand | Display State | Cohort | Unlock Mechanism | Built-blocked? | Intentional restore? | Accidental vs explicit restore? |
| --- | --- | --- | --- | --- | --- | --- |
| ascend | active_profile_ready | restored_legacy_public | legacy_visibility_unlock / restored_legacy_public (visibility-fix pack) | false | true | No |
| comfort-inn-suites | draft_applied_with_defects | restored_legacy_public | legacy_visibility_unlock / restored_legacy_public (visibility-fix pack) | false | true | No |
| country-inn-suites | legacy_approved_pending_migration | restored_legacy_public | legacy_visibility_unlock (historicalApproved + presentation + visuals + image uniqueness) | true | false | Yes — public-full via legacyVisibilityUnlock, not an explicit public-restore command |
| curio-collection | active_profile_ready | restored_legacy_public | legacy_visibility_unlock / restored_legacy_public (visibility-fix pack) | false | true | No |
| design-hotels | active_profile_ready | primary_release | primary_release | false | true | No |
| everhome-suites | active_profile_ready | primary_release | primary_release | false | true | No |
| hotel-indigo | active_profile_ready | primary_release | primary_release | false | true | No |
| kimpton | active_profile_ready | primary_release | primary_release | false | true | No |
| mgallery-collection | active_profile_ready | primary_release | primary_release | false | true | No |
| radisson-individuals-by-choice | active_profile_ready | primary_release | primary_release | false | true | No |
| small-luxury-hotels-of-the-world | active_profile_ready | primary_release | primary_release | false | true | No |
| suburban-studios | legacy_approved_pending_migration | restored_legacy_public | legacy_visibility_unlock (historicalApproved + presentation + visuals + image uniqueness) | true | false | Yes — public-full via legacyVisibilityUnlock, not an explicit public-restore command |
| tribute-portfolio | active_profile_ready | restored_legacy_public | legacy_visibility_unlock / restored_legacy_public (visibility-fix pack) | false | true | No |
| woodspring-suites | legacy_approved_pending_migration | restored_legacy_public | legacy_visibility_unlock (historicalApproved + presentation + visuals + image uniqueness) | true | false | Yes — public-full via legacyVisibilityUnlock, not an explicit public-restore command |

Built-blocked not yet public-full: quality-inn, radisson, radisson-blu, radisson-red
