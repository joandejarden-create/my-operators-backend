# Hilton ADP Client Share

**Hotel:** Hilton New York Times Square  
**ADP subject:** `adp_hilton_times_square`  
**HPC:** `rec35fExUxCClpOP6`  
**Token id:** `sht_2fa10cb1596f38204b39fd61`  
**Published period:** `adp_period_adp_hilton_times_square_20261005122652_63a1d8`

## Client URL

https://my-operators-backend-production.up.railway.app/owner-ai-demand-share.html?share=adpshare.v1.eyJ2IjoxLCJ0aWQiOiJzaHRfMmZhMTBjYjE1OTZmMzgyMDRiMzlmZDYxIiwicHJvcGVydHlJZCI6ImFkcF9oaWx0b25fdGltZXNfc3F1YXJlIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwcm9wZXJ0aWVzIiwicHVibGljYXRpb25fbWV0YSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzkxMzA4NTY2LCJleHAiOm51bGx9.ns3cLGLM7tBp4dEAxUp2orhMVmCJ95eKRWBZ7XRm4hw

## Admin

Sealed into `config/client-share/production-share-contract-tokens.json` so local Admin ADP Client Open/Copy can hydrate without minting.

Census link added: `adp_hilton_times_square` → `rec35fExUxCClpOP6`.

## Deploy note

Production share verify reads `config/client-share/adp-share-registry/active-tokens.json`.  
After mint, deploy so Railway has token `sht_2fa10cb1596f38204b39fd61` ACTIVE, then smoke the URL logged-out.
