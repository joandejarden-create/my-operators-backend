# GLOBAL Surfe / missing-channel enrichment — founder clarification response

## Scope

The Surfe integration correction is **global**, shared by Hotel Explorer and GDI via:

- `lib/shared-contact-intelligence/missing-channel-enrichment-v3.js`
- `lib/surfe/client.js` (`linkedinUrl` builder, landline/mobile normalize)
- `lib/market-alerts-contact/surfe-adapter.js` (HE Surfe adapter)
- `lib/group-demand-intelligence/gdi-surfe-contact-provider-v1.js` (GDI Surfe adapter)

Bethesda Marriott / Meinrad Lang III remain **golden-reference tests only**. Production paths contain no hotel- or person-specific Surfe special cases.

## Acceptance mapping

| Requirement | Evidence |
|---|---|
| All five Bethesda contacts | `he-contact-dynamic-enrich-bethesda-golden-v3` (live Airtable read) |
| All email/phone missing combos | `test-shared-missing-channel-enrichment-global-v3` matrix |
| Verified LinkedIn identity | Shared `buildSurfeEnrichPersonInput` → `linkedinUrl` |
| Correct Surfe include construction | `surfeIncludeFromMissingState` / `surfeIncludeForChannel` |
| Async / partial / skip | HE reveal pending ≠ NOT_FOUND; GDI skip when complete |
| No unnecessary credits | Phone-only → email=false; email-only → mobile=false; complete → no call |
| No Surfe PII persistence | Shared Surfe transient policy + reveal `NOT_WRITTEN` / `persisted:false` |
| Shared HE + GDI behavior | Same missing-channel module; include-flag parity gate |
| Multi-market representative | Synthetic 3-market / 3-hotel / 8-person cohort (no real PII) |

## Non-resolution note

Success of any single golden person (including Meinrad) is **not** sufficient. Permanent gates require the shared matrix + HE/GDI parity + golden cohort.
