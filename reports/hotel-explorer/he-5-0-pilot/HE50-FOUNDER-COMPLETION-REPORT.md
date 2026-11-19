# HE 5.0 — Eight-Hotel Research Pilot Founder Report

**Status:** STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.  
**Run ID:** `he50_mullen_eight_hotel_2026_11_19`  
**PR:** [#463](https://github.com/joandejarden-create/my-operators-backend/pull/463) (OPEN)  
**Paid spend confirmed:** $0.00 · **Reserved:** $0.00 · **Remaining authorized:** $50.00 of $50.00  
**Free spend:** $0.00  
**Airtable writes executed:** 0  
**Persistence classification:** `PERSISTENCE_DESIGNED` (not FULL_E2E_VALIDATED)

---

## 1. Infrastructure audit

See `HE50-INFRASTRUCTURE-INVENTORY.md`.

## 2. Field-to-Airtable mapping

See `HE50-FIELD-GAP-MATRIX.md` (48 Bethesda-seed fields × 12 tabs).

## 3. Provider routing

See `HE50-PROVIDER-ROUTING.md`. Paid providers **not invoked** this wave (free-first complete).

## 4–10. Research findings (summary)

### Bethesda (Wave A)

- **Year built:** 1979 (Travel Weekly / CMBS / Cvent) — staged HPC null-fill.
- **Renovation:** SOURCE_CONFLICT — major reno ~2015 vs guestroom unveil press 2017; evidence-only dual dating.
- **Rooms:** 407 reused / corroborated.
- **Ownership:** Legal = Marbeth Acquisition LLC (SDAT); Economic sponsor = Mullen REC (staged MEDIUM); Operator/Brand = Marriott live; current lender / UBO unresolved.
- **CMBS note:** $39.8M loan terms → evidence only (no invented lender edge).
- **Demand / Access:** Infrastructure connected; US anchors sparse — honest empty.
- **Guest Sentiment:** Staged founder-preview; licensing-gated.
- **Market Alerts news:** Genuinely absent for Bethesda.
- **Contacts:** Public identity staged; Surfe not used.

### Seven candidates (Wave B)

| Hotel | Identity | HPC / Census | Rooms (free) | Action |
| --- | --- | --- | --- | --- |
| AC Hotel Newtown Square | NEW_HPC | — | 140 (conflict alt 146) | CREATE proposal |
| Carton House Fairmont | NEW_HPC | — | 169 HIGH | CREATE proposal |
| HGI Newtown Square Radnor | NEW_HPC | — | 138 MEDIUM | CREATE proposal |
| JW Marriott Costa Elena | MATCHED_HPC | `recq9KWr8UE7z2549` | 415 HIGH | UPDATE alias/rooms |
| Secrets Cap Cana | MATCHED_HPC | `recd7XAM6tLjzQhaI` | reuse HPC | Confirm not Dreams Cap Cana |
| Secrets Papagayo | NEW_HPC_CANDIDATE | — (legacy provenance `recyX1wLsHBJYRPw7` only) | 202 HIGH | HPC CREATE — no census runtime id |
| Secrets St. Lucia | NEW_HPC | — | 355 HIGH | CREATE proposal |

**Mullen holdings:** Not auto-verified. All non-Bethesda associations remain `PROPOSED_PORTFOLIO_CANDIDATE_NOT_VERIFIED_HOLDING` (JW trade press claims flagged VERIFY_BEFORE_GRAPH).

## 11. Dry-run package

`HE50-AIRTABLE-DRY-RUN-PACKAGE.json` — 12 mutations · `applyAuthorized: false`.

## 12. Cost ledger

`HE50-RESEARCH-SPEND-LEDGER.json`

| Metric | Value |
| --- | ---: |
| Confirmed paid | $0.00 |
| Reserved | $0.00 |
| Remaining of $50 | $50.00 |
| Newly verified facts (agg) | 22 |
| Cost per verified useful field | n/a (paid=$0) |

## 13. Expected post-apply HE completeness

| Hotel | After approved apply (expected) |
| --- | --- |
| Bethesda | Overview + rooms + ownership/contacts/sources substantially populated from staged graph; Demand/News remain thin until anchors/alerts exist |
| Matched HPC (JW, Cap Cana) | Identity + rooms/alias; HE deep-link available |
| Papagayo | After HPC CREATE: deep-link + min identity (legacy census id not canonical) |
| NEW_HPC ×5 (incl. Papagayo) | After CREATE: Founder Preview deep-links + min HPC fields only |

## 14. Automated tests

`npm run test:he-5-0-full-e2e-pilot-v1`

## 15. Preview links

- Bethesda Marriott: http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recLuxvwwxID7U2B8&previewMode=proposed
- JW Marriott Costa Elena Resort All-Inclusive: http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recq9KWr8UE7z2549&previewMode=proposed
- Secrets Cap Cana Resort & Spa: http://127.0.0.1:8091/hotel-explorer-preview.html?hotelExplorer=recd7XAM6tLjzQhaI&previewMode=proposed

## 16. Required scorecard

| Hotel | HPC identity | Newly verified facts | Existing facts reused | Proposed Airtable changes | Research spend | Remaining gaps |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Bethesda Marriott | MATCHED_HPC | 1 | 20 | 4 | $0.00 | current_lender; equity_jv; hotel_news_market_alerts |
| AC Hotel Newtown Square | NEW_HPC_CANDIDATE | 3 | 0 | 1 | $0.00 | full_he_population; ownership_verification; contacts |
| Carton House, a Fairmont Hotel | NEW_HPC_CANDIDATE | 3 | 0 | 1 | $0.00 | full_he_population; ownership_verification; contacts |
| Hilton Garden Inn Newtown Square Radnor | NEW_HPC_CANDIDATE | 3 | 0 | 1 | $0.00 | full_he_population; ownership_verification; contacts |
| JW Marriott Costa Elena Resort All-Inclusive | MATCHED_HPC | 3 | 5 | 1 | $0.00 | full_he_population; ownership_verification; contacts |
| Secrets Cap Cana Resort & Spa | MATCHED_HPC | 3 | 5 | 1 | $0.00 | full_he_population; ownership_verification; contacts |
| Secrets Papagayo Costa Rica | NEW_HPC_CANDIDATE | 3 | 0 | 1 | $0.00 | full_he_population; ownership_verification; contacts |
| Secrets St. Lucia | NEW_HPC_CANDIDATE | 3 | 0 | 1 | $0.00 | full_he_population; ownership_verification; contacts |

### Aggregate

- MATCHED_HPC: 3 · MATCHED_HOTEL_CENSUS: 0 · NEW_HPC_CANDIDATE: 5
- Proposed mutations: 11
- Paid committed: $0.00

## Founder decisions required

1. Approve READY mutations only (10 READY · 1 HOLD · 1 REJECT) — see `HE50-MUTATION-RECONCILIATION-V1.json`.
2. Decide whether to spend any of remaining $50 on current-lender / UBO / Full HE gaps (not identity — Cap Cana + AC rooms resolved free).
3. Explicit production Airtable apply authorization (separate from this $50 research auth).
4. Steward review lowercase Cap Cana alias `recFP9eslejVXbyuR` (separate from Dreams).
5. Optional: strip HGI rooms on CREATE if HIGH-only keys policy preferred.

---

## STOP — AWAITING SEPARATE FOUNDER AUTHORIZATION FOR PRODUCTION AIRTABLE APPLY.

Do not interpret successful research as permission to publish or persist.
