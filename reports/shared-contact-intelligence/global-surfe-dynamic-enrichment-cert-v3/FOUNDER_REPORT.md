# GLOBAL Surfe Dynamic Enrichment — Founder Certification Report

**Classification:** `GLOBAL_SURFE_ENRICHMENT_VALIDATED`  
**At:** 2026-11-28T11:09Z  
**Preserve:** dirty GDI/HE/IC worktrees untouched for product homes; no Airtable Surfe PII writes; no production deploy.

## Integration status

| PR | Status | Notes |
|---|---|---|
| #696 | MERGED | Contact Intelligence V2 visibility / hybrid baseline preserved |
| #697 | MERGED | Bethesda phone E2E / Findymail durable phones preserved |
| #699 | MERGED (`098bb459a`) | Surfe `linkedinUrl` + landline/mobile session parity |
| #700 | MERGED (`ab751fc18`) | Dynamic missing-channel V3 (shared HE+GDI) |
| Follow-up cert tip | OPEN after #700 | `cert(contact): global Surfe dynamic enrichment validation` — Findymail≠Surfe messaging + `GLOBAL_SURFE_ENRICHMENT_VALIDATED` evidence |

IC owns merge of the certification follow-up PR. Product chats do not merge.

## Golden root cause (Meinrad Lang III · Bethesda Marriott)

Live Surfe API (credit-guarded diagnostic `5b090384-c17e-4886-b916-eef78ffec3e4`):

| Factor | Finding |
|---|---|
| Identity input | Fixed — must send `linkedinUrl` (not `linkedInUrl`) |
| Requested channels | Historically wrong — `include.mobile` defaulted false; phone lives in `mobilePhones` |
| API vs extension | Same coverage when LinkedIn + mobile requested |
| Async handling | Poll until `COMPLETED` (`pollSurfePeopleEnrichment`) |
| Response parsing | Now reads `emails[]`, top-level email, `mobilePhones`, `landlines` |
| Error classification | Pending ≠ no-result; Findymail miss ≠ Surfe miss |

**Live outcome:** email `m***g@marriott.com` + mobile `********2451` · credits consumed email **1** + mobile **1** · **NOT written to Airtable**.

No founder screenshot values hardcoded.

## Bethesda five — dynamic channel selection (Airtable SoT)

| Person | Email | Phone | Surfe request |
|---|---|---|---|
| Jeff Mullen | present | missing | **phone only** (`email:false`, `mobile:true`) |
| Katherine Button | present | present | **none** |
| Patrick Reece | present | present | **none** |
| Meinrad Lang III | missing | missing | **both** |
| Jenny Pyne | missing | missing | **both** |

## Combo coverage

- Mock matrix: none / phone / email / both — **PASS**
- Multi-market representative gate: 3 markets · 8 people · channels `{none:1, phone:2, email:2, both:3}` — **PASS**
- Other HE50 HPC IDs currently have **0** HI Org Contacts — channel combos covered via Bethesda + shared global gate (not hotel-hardcoded PII)

## Credits independence

- Email and mobile credits tracked separately (`totalEmail` / `totalMobile`)
- Phone-only Surfe include skips email credits
- Email-only Surfe include skips mobile credits
- Meinrad live spend: email −1, mobile −1

## GDI / HE parity

Shared module: `lib/shared-contact-intelligence/missing-channel-enrichment-v3.js`  
Used by HE hybrid/reveal API and GDI Surfe provider path. Session-only Surfe paint; Findymail durable path unchanged.

## Browser / authenticated UI

Served feature tip on **`:8096`** (canonical `:8080` remains dirty `ws-gdi` — preserved).

| Gate | Result |
|---|---|
| Find-button visibility (5 viewports) | **PASS** |
| Meinrad visible + hybrid Find buttons | **PASS** (5× `[data-hci-hybrid-enrich]`) |
| Session-only Surfe click | Not auto-fired (credit guard); provider proven via live diagnostic |

## Gates run

- `npm run certify:global-surfe-dynamic-enrichment-v3`
- `npm run test:shared-missing-channel-enrichment-global-v3`
- `npm run test:he-contact-dynamic-enrich-v3`
- `npm run test:he-surfe-contact-parity-v1`
- Visibility + Surfe UI Playwright certs against `:8096`

## Final state

`GLOBAL_SURFE_ENRICHMENT_VALIDATED` — provider behavior verified with real Surfe request/response for the golden person; dynamic missing-channel selection certified globally via shared module + Bethesda five + multi-market matrix.

**AWAITING_INTEGRATION_CONTROLLER** for merge of #699 then #700.
