# Fresh Hotel Certification Proposal

> **PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND**

## Recommended hotel: The Westin Grand München

- **Canonical hotel ID:** `recFaxTEFF9ILHWC9`
- **Market:** Munich, Germany — Munich / Arabellapark / Bogenhausen
- **Profile:** 627 rooms · 21 meeting rooms · 2810 sqm meeting space · 1053 sqm ballroom · urban_meetings_congress_business
- **State:** no STAGED, no CURRENT, no publication package (not Wyndham; Wyndham `recnPB3Q02NO8Ma1o` excluded)
- **Local pilot data:** 26 opportunities ({"WATCHLIST":21,"DISQUALIFIED":5}), 0 with email — never published

## Why this hotel

- Has not completed full GDI; genuinely fresh for a hotel-agnostic Live proof.
- Different market type (German-primary urban meetings/congress) than the Wyndham / US baseline.
- ADP intelligence already exists for this property — cross-reference only.
- Rejected/avoided: recnPB3Q02NO8Ma1o, recLuxvwwxID7U2B8, recH9h3McgMNFQEla, recrPQcZg7SFARRb2, rec1t35e3sPerzxYu.

## Applicable mechanisms

| Mechanism | Behavior | Spend |
|---|---|---|
| DEMAND_DISCOVERY | Fourteen-segment discovery via canonical chain when artifacts missing + -AuthorizePaidDiscovery (NEW HOTEL FULL E2E). Artifacts present → reuse. | Exa discovery, ≤ shared ceiling |
| CAD / PLP | CAD classification + PLP embedded in discovery; classification only. | included in discovery |
| LEISURE (LDI) | Standard stage on Live spine when enabled; mock_live only ($0) unless separately authorized. | $0 mock |
| OPPORTUNITY_DEEP_RESEARCH | Same paid-discovery authorization triggers canonical deep when dossiers missing (shared chain run with discovery — no double spend). | included in discovery/deep spend |
| WHO_HOW_RESOLUTION | Existing Exa agent adapter, Checkpoint A pattern; requires authorization id + ceiling + manifest + GDI_EXA_AGENT_LIVE=1. | Exa ≤ $25 hard ceiling (shared) |
| PROFESSIONAL_CONTACT_RESEARCH | Public-first named-contact channels; Findymail only when authorized (FINDYMAIL_API_KEY). | Findymail ≤ 15 credits via -FindymailCreditHardCap 15 |
| COMMERCIAL_QUALIFICATION | Strict FQO + entry-point reachability + buying-authority annotation (in memory). | none |
| STAGED progression | Package build + validation; STAGED activation only if founder includes it in the GO. CURRENT never changes. | none |

## Expected scope (estimates, not quotes)

- Starting point: 26 pre-existing local pilot opportunities ({"WATCHLIST":21,"DISQUALIFIED":5}), 0 with email — not customer-published, no STAGED.
- Target: ≈ 15–30 qualified commercial opportunities (strict FQO; may be fewer — budget exhaustion ≠ COMPLETE)
- WHO/HOW: Top-ranked opportunities only, bounded by the $25 Exa ceiling
- Contacts: Named buyer/entry contacts for qualified opportunities; Findymail only for eligible full-name + domain candidates

## Proposed limits

| Item | Limit |
|---|---|
| Exa (discovery + deep + WHO/HOW) | **$25 hard ceiling** |
| Findymail | **15 credits** (`-FindymailCreditHardCap 15`) |
| Surfe | **FORBIDDEN** |
| CURRENT | never changed |
| STAGED | activation only if explicitly authorized; otherwise `-NoActivateStagedPointer` |

## Airtable write scope

- Default: **none**. `-AuthorizePaidResearch` never unlocks writes.
- If separately authorized: Group Demand Opportunities upsert for hotelId recFaxTEFF9ILHWC9 only (idempotent, dry-run first via -PlanResearchPersistence), exact write-set text required, GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1. No schema changes, no other hotels, no CURRENT.

## Prerequisites and risks

- NEW HOTEL FULL E2E: missing discovery/deep artifacts + -AuthorizePaidDiscovery + -AuthorizePaidResearch + -AuthorizationId + -HardCeilingUsd + EXA_API_KEY + GDI_EXA_AGENT_LIVE=1 → canonical chain runs automatically. Without -AuthorizePaidDiscovery, Stages 1–2 HOLD with …_MISSING_PAID_NOT_AUTHORIZED.
- Shared Exa ceiling covers discovery + deep + WHO/HOW; budget exhaustion ≠ COMPLETE.
- A tranche manifest (01-TRANCHE_MANIFEST.json) must be authored for this hotel; no Wyndham manifest may be reused.
- Findymail credit hard cap: pass -FindymailCreditHardCap 15 (--findymail-credit-hard-cap 15).
- Re-run the pre-live gate before GO. This proposal is NOT spend authorization.

## Commands

Pre-flight (no spend):

```powershell
npm run gdi:unified-live-e2e-prelive-readiness-v1
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId recFaxTEFF9ILHWC9 -Mode DryRun
npm run gdi:weekly-refresh -- --hotel-id recFaxTEFF9ILHWC9 --dry-run
```

Live FULL — NEW HOTEL (founder GO required — do not run before authorization):

```powershell
$env:GDI_EXA_AGENT_LIVE = '1'          # Exa spend gate (set only after founder GO)
# $env:FINDYMAIL_API_KEY must already be present in the session (never echo it)
pwsh -File scripts/gdi/Invoke-GdiFullE2E.ps1 -HotelId recFaxTEFF9ILHWC9 -Mode Live `
  -AuthorizePaidResearch -AuthorizePaidDiscovery `
  -AuthorizationId 'FOUNDER_GO_GDI_LIVE_E2E_WESTIN_MUNICH_20261128' -HardCeilingUsd 25 `
  -Manifest 'reports/gdi-fresh-hotel-live-e2e-recFaxTEFF9ILHWC9-20261128/01-TRANCHE_MANIFEST.json' -OutDir 'reports/gdi-fresh-hotel-live-e2e-recFaxTEFF9ILHWC9-20261128' `
  -AuthorizeFindymail -FindymailCreditHardCap 15 -EnableLeisure -EnableCad -NoActivateStagedPointer
# -NoActivateStagedPointer: drop only if the founder authorizes STAGED activation in the same GO
# Do NOT pass -AllowHistoricalReplay for a genuine new-hotel paid run
```

## Proposed authorization token

`FOUNDER_GO_GDI_LIVE_E2E_WESTIN_MUNICH_20261128`

Identifier only. It authorizes nothing until the founder issues it in writing.

> **PROPOSED LIMITS — NOT AUTHORIZATION TO SPEND**
