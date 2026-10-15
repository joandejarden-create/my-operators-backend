# Private Events — record-level proof (Mayflower + San Diego Bayside)

**Authoritative cwd:** `C:\Dev\dealality\gdi-release-main`  
**Generated:** 2026-10-15  
**Airtable base:** `appa2cE7FTRmIbB32`  
**Production host:** `https://my-operators-backend-production.up.railway.app`

## Summary

| Hotel | Venue | Venue ID | Venue Airtable | Signal IDs | Fit ID | Fit Airtable | GDI opportunity ID | Published | Reason |
|-------|-------|----------|----------------|------------|--------|--------------|--------------------|-----------|--------|
| Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o` | USS Midway Museum | `pev_7aa5dae26f771584` | [recifgsbla66GJXdB](https://airtable.com/appa2cE7FTRmIbB32/recifgsbla66GJXdB) | `pes_1695ba0a0e800e` ([recih229CC8OiX8R8](https://airtable.com/appa2cE7FTRmIbB32/recih229CC8OiX8R8)) | `hvf_3200e740958b1a` | [recR9glMYAtQXMQTV](https://airtable.com/appa2cE7FTRmIbB32/recR9glMYAtQXMQTV) | `gdi_opp_prospect_mv8p5ddk_c3ef825a` | **published** | Existing Midway GDI card updated + linked; evening-events program + no on-site lodging; core North Harbor catchment |
| Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o` | Embarcadero Marina Park South | `pev_4d8c16746660b2ec` | [recQanpj0i1igEjco](https://airtable.com/appa2cE7FTRmIbB32/recQanpj0i1igEjco) | `pes_6ddccbef26c948` ([rec4Yaatyj1XibqIx](https://airtable.com/appa2cE7FTRmIbB32/rec4Yaatyj1XibqIx)) | `hvf_71f3112abb962c` | [recWNJxkuMWZFQpVJ](https://airtable.com/appa2cE7FTRmIbB32/recWNJxkuMWZFQpVJ) | `gdi_opp_prospect_mv8p5ddl_acb56fba` | **published** | Existing EMPS GDI card updated + linked; Port permit program; no on-site lodging |
| Mayflower `rec0eTEnCqlsGJJt9` | Anderson House | `pev_097b4cd1fa2280c2` | [recV4IeQBVPkNZiOv](https://airtable.com/appa2cE7FTRmIbB32/recV4IeQBVPkNZiOv) | `pes_4cb8495e9ccfbf` ([reczOnwZGywFq7maR](https://airtable.com/appa2cE7FTRmIbB32/reczOnwZGywFq7maR)) | `hvf_f427d6324a797c` | [recjA0xmxXbt5TEC6](https://airtable.com/appa2cE7FTRmIbB32/recjA0xmxXbt5TEC6) | `gdi_pe_1e87cdb00b80fc5f` | **published** | Mayflower-local historic house venue; named contact path; Non-exclusive hotel partners identified |
| Mayflower `rec0eTEnCqlsGJJt9` | DAR Headquarters | `pev_177defed35e357e0` | [recMtpyOLdXpYUcmr](https://airtable.com/appa2cE7FTRmIbB32/recMtpyOLdXpYUcmr) | `pes_4175ca63e896a9` ([rec6CuDSYrpASYnzZ](https://airtable.com/appa2cE7FTRmIbB32/rec6CuDSYrpASYnzZ)) | `hvf_76007339df08b8` | [rec5f05YJCLrcWgy1](https://airtable.com/appa2cE7FTRmIbB32/rec5f05YJCLrcWgy1) | `gdi_pe_b4de27f6743c03c5` | **published** | Mayflower-local DAR HQ event spaces; organization contact path; no exclusive lodging partner published |

## Production API read-back

Customer list (post visibility fix: `fitExplanation` + `REVIEWED_PACKAGE_V1` + `prospectHotelFacing`):

- San Diego: **21** facing cards including Midway + Embarcadero
- Mayflower: **13** facing cards including Anderson + DAR

Detail endpoints (all HTTP 200, `detailLayout=VENUE_PARTNERSHIP`, venue/fit/signal IDs present):

- Midway: `/api/group-demand-intelligence/hotels/recnPB3Q02NO8Ma1o/opportunities/gdi_opp_prospect_mv8p5ddk_c3ef825a`
- Embarcadero: `.../gdi_opp_prospect_mv8p5ddl_acb56fba`
- Anderson: `/api/group-demand-intelligence/hotels/rec0eTEnCqlsGJJt9/opportunities/gdi_pe_1e87cdb00b80fc5f`
- DAR: `.../gdi_pe_b4de27f6743c03c5`

## Idempotency

Coordinator stage `apply_pe_canonical` + persist CLI `--rerun`: venue/fit/signal **update**, opportunityMatch **UPDATE** only — no duplicate GDI cards for Midway/Embarcadero.

## Paid research gap (unapproved)

No additional paid Parallel/Webhound runs. Candidates not yet published remain only those lacking verified contact routes or hotel-specific activity evidence beyond the four packages above.
