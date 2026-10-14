# Production change set — GDI reviewed prospect publication

**Status:** **Published** — Airtable applied; code deploy `cb4bd52d`; flag ON for Bethesda + Mayflower  
**Date:** 2026-10-14

## 1. Deployed code

- Branch: `gdi/prospect-publication-bethesda-mayflower`
- Deploy SHA (working tree upload): `9b121257` (merge tip) + uncommitted/hotfix tree contents via `railway up`
- Railway deployment: `a9ef91de-3a7e-4cc3-a082-469d7682a310` (**SUCCESS**)
- Flag redeploy (env set): `4a5119f9-abc4-4303-b64a-1e4ac7ae7cc7` (triggered by `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=1`)

### Runtime configuration

| Variable | Value |
|----------|-------|
| `GDI_OPPORTUNITIES_PERSISTENCE` | `airtable` (production) |
| `AIRTABLE_GDI_BASE_ID` | `appa2cE7FTRmIbB32` |
| `GDI_REVIEWED_PROSPECT_PUBLICATION_V1` | set to `1` (awaiting redeploy SUCCESS) |
| `GDI_REVIEWED_PROSPECT_HOTEL_IDS` | `recLuxvwwxID7U2B8,rec0eTEnCqlsGJJt9` |

## 2. Adapter & schema

- **Authoritative store:** Airtable GDI base via `opportunityPayloadJson` fidelity (`lib/group-demand-intelligence/airtable-opportunity-store.js` + `opportunity-persistence.js`)
- **Not** filesystem SoT in production (FS mirror may exist; customer path uses Airtable)
- Feedback / hotel validation: share-validation store + Decision Outcomes dual-write
- Publication metadata: `prospectPublicationBasis=REVIEWED_PACKAGE_V1`, section, contact stamps on opportunity payload

## 3. Package hashes

| Artifact | SHA-256 |
|----------|---------|
| Register | `f5f348d67f26a06e5337f511175c6b1e65d42f64f2b04a4ef7bfcccf90bf1c3e` |
| Contact packages | `b5ff071ab8e97050710553330d89d62a578854ecc3b71456f45819665bb70be3` |

### Expected reviewed membership

| Hotel | Priority | Exploratory | Internal enrichment |
|-------|----------|-------------|---------------------|
| Bethesda | 11 | 2 | 1 (CTN) |
| Mayflower | 9 | 2 | 4 |

## 4. Backups / rollback

- Pre-apply backups: `reports/gdi-prospect-publication-implementation/production-apply/backups/`
- Rerun backups: `production-apply-rerun/backups/`
- Rollback order: (1) set `GDI_REVIEWED_PROSPECT_PUBLICATION_V1=0` (2) restore only conflicting opportunity fields from backup while preserving post-import customer validation/feedback (3) do not wholesale-delete unmatched historical rows

## 5. Auth feedback (production)

- Share-capability path verified for Bethesda contract hotel (authorized write 200; reload preserves note; no-token 403; cross-hotel 403 `SHARE_HOTEL_SCOPE`)
- Reversible note on `gdi_opp_ndss_advocacy_2027` (familiarity `ALREADY_KNOWN` only — no fictitious “worth pursuing”)
- Memberstack browser Save JWT path still requires interactive hotel-user sign-in (not claimed complete here)

## 6. Customer review routes (no secrets)

- Production host: `https://my-operators-backend-production.up.railway.app`
- Bethesda GDI page: `/group-demand-intelligence.html?hotelId=recLuxvwwxID7U2B8&share=<contract-token>`
- Mayflower GDI page (authenticated/pilot-read): `/group-demand-intelligence.html?hotelId=rec0eTEnCqlsGJJt9`
- Do not paste share tokens into reports
