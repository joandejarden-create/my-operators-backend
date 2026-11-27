# Findymail implementation audit (HE / GDI / shared)

**Date:** 2026-11-27  
**Scope:** Hotel Explorer Contact Intelligence CX V2 — establish facts, do not assume integration  
**Paid calls this cycle:** **none**

## Verdict

| Question | Answer |
|---|---|
| Is Findymail already integrated? | **No** — no provider adapter, API client, env credential wiring, or HE/GDI runtime path that calls Findymail |
| Is it enabled? | **No** — workflows hard-code `findymail: false` / refuse activation |
| Which workflows use it? | **None** execute Findymail. GDI contact-research stages only *mention* it as an optional future gate that remains OFF |
| What data does it return? | **N/A** — not invoked; no observed payloads in-repo |
| What verification does it provide? | **N/A** |
| Actual cost? | **$0** incurred; no ledger line items for live Findymail spend found under HE |
| Retention / persistence rights? | **Unknown / not contracted** — no Terms artifact or standing-auth allowlist entry for Findymail persistence |
| Stored in canonical Airtable? | **No** Findymail-sourced channel rows identified; HE standing auth forbids Surfe persistence and does not authorize Findymail |

## Evidence scanned

- Hotel Explorer workspace: **zero** `findymail` / `Findymail` / `FINDYMAIL` matches under `lib/`, `api/`, `public/`, fixtures
- Shared contact research: Surfe reveal path exists (`/api/contact-intelligence/.../reveal`); Findymail does not
- GDI: numerous workflow flags (`authorizeFindymail`, `findymailUsed: false`, `findymailActivation: "OFF"`) that **refuse** paid Findymail rather than call it
- Provider policy: `gdi-public-contact-research-policy-v2.js` lists `findymail` under `paidProvidersForbidden`

## Shared integration proposal (not implemented)

Do **not** create a second Hotel Explorer contact engine.

If founder later authorizes Findymail:

1. Add one shared provider adapter under existing contact-intelligence / GDI public-contact provider registry (same pattern as Surfe).
2. Explicit-click only; session-first display; persistence only after rights review + standing-auth allowlist.
3. Map returns into HI Contact Channels with provider=`findymail`, verification=`PROVIDER_RETURNED_UNREVIEWED` until human/admin confirmation.
4. Deduplicate against existing person + channel keys; never invent pattern emails.
5. Budget ledger + credentials (`FINDYMAIL_API_KEY`) + written ToS/retention memo before first paid call.
6. HE UI continues to use **Find Contact Details** → shared reveal workflow (provider-pluggable), not a Findymail-specific button.

## Relation to CX V2

Customer UI now uses **Find Contact Details** for the authorized on-demand resolution path (currently Surfe / Surfe-mock when configured). Findymail remains **absent and disabled** until a separate authorization cycle.
