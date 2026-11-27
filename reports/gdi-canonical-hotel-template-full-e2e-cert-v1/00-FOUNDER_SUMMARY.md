# GDI Canonical Hotel Template & Full E2E Certification

Status: **PARTIAL — INTEGRATED_ON_8080 — AWAITING_FOUNDER_REVIEW**

Not claimed: `GDI_CANONICAL_HOTEL_TEMPLATE_AND_FULL_E2E_CERTIFIED` (best-effort gate + Live V3 orchestrator + clean Airtable-only reconstruction remain open).

STAGED: `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_59997e61`  
Workspace tip at certification: `fbaa0a27b` (pre-merge)

## 1. Permanent hotel template

- Contract: `GDI_CANONICAL_HOTEL_TEMPLATE_V1`
- Module: `lib/group-demand-intelligence/presentation/gdi-canonical-hotel-template-v1.js`
- Doc: `docs/ai-build-system/GDI_CANONICAL_HOTEL_TEMPLATE_V1.md`
- Gate: `npm run test:gdi-canonical-hotel-template-v1` — PASS
- UI freeze: no visual changes

## 2. E2E operational readiness

See `01-E2E_OPERATIONAL_READINESS_MATRIX.json` (17 mechanisms: 1 OPERATIONAL_CERTIFIED auto-STAGED, 1 GAP unified V3 Live, 15 intermediate).

## 3–4. Wyndham contact audit + best-effort ledger

### Primary contact coverage (STAGED 26) — refreshed

| Channel | Coverage |
|---|---|
| Email | 19/26 |
| Telephone | 7/26 |
| LinkedIn | 14/26 |
| ≥1 outreach channel | 26/26 |
| Abbreviated primary | 1 (`Suzanne K.`) |
| List/Drawer presentation drift | 0 |

### Unique-person scorecard (primary + alts)

- Unique commercial contacts: 40
- Email verified: 29 · Telephone: 7 · LinkedIn: 28
- Email/phone usable: 30 · Usable outreach: 34
- Findymail eligible remaining: **0** (ledger + prior enrichment)
- Authority annotations missing: 26/26
- Opportunities with alternative contacts: 14/26

## 5–9. WHO/HOW, recovery, Findymail, gap research

- No rediscovery of the original 161 candidates
- Existing Airtable contact rows for Wyndham STAGED 26: **0 missing**
- Findymail Live **not** executed (no incremental authorization this cycle; eligible remaining = 0)
- Paid Exa/telephone/LinkedIn continuation **not** executed
- Prioritized gap manifest: `06-PRIORITIZED_CONTACT_GAP_MANIFEST.json` (26 unique persons with ≥1 unresolved channel research marker)

## 10–12. Persistence, reconstruction, List/Drawer

- Airtable read presence for STAGED 26 opportunity IDs: PASS (`airtableMissing=0`)
- Clean Airtable-only reconstruction: **PARTIAL** — not executed; see `07-CLEAN_RECONSTRUCTION_STATUS.json`
- List ↔ primary presentation parity: PASS
- Drawer opened on `:8080` for Bora / Lily Yuan (full name + email + LinkedIn)

## 13–15. Research Receipt, expenditure, remaining requirements

- Research Receipt present on ES (`Dealality Research Receipt` + methodology ranges)
- Expenditure this cycle: Findymail 0 · Exa $0 · Surfe 0
- Outstanding for CERTIFIED:
  1. Complete per-channel research markers (email 4 / phone 14 / LinkedIn 12 unique NOT_YET)
  2. Annotate buying authority on all 26 opportunities
  3. Close unified V3 Live orchestrator GAP
  4. Execute clean Airtable-only reconstruction proof
  5. Founder-authorized incremental budget if further public-source/Exa gap recovery is required

## 16. Canonical PowerShell

```powershell
pwsh -File scripts/gdi/Invoke-GdiDiscovery.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiDeepResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiQualification.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiValidateFullE2E.ps1 -HotelId recnPB3Q02NO8Ma1o -RequirePort8080
node scripts/gdi-canonical-hotel-template-full-e2e-cert-v1.mjs
# Live Findymail only with explicit founder auth:
# pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId recnPB3Q02NO8Ma1o -Mode Live -AuthorizePaidResearch -AuthorizeFindymail
```

## 17–20. Rollout, tests, merge, :8080

- Hotel rollout readiness: `05-HOTEL_ROLLOUT_READINESS.json`
- Regression: `test:gdi-canonical-hotel-template-v1`, `test:gdi-contact-research-best-effort-v1` (also wired into `Invoke-GdiValidateFullE2E.ps1`)
- Browser evidence: `08-8080_BROWSER_EVIDENCE.json` + `screenshots/`
- Merge SHA / serving commit: filled after IC remote merge
