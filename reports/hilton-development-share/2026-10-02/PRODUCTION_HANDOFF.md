# Hilton Development — Production External Share Handoff

**Date:** 2026-10-03 (V3 envelope recovery)  
**Verified timestamp:** 2026-10-03T13:19:40.299Z (UTC)  
**Environment:** production  
**Production host:** https://my-operators-backend-production.up.railway.app  
**Production SHA:** `3544ed38d162f3b7bedf8c397e56f047644aeab6`  
**Railway deployment:** `732672e9-b3dd-4d41-b989-c0639af50f5e` (SUCCESS)  
**Clean-browser QA:** PASS (logged-out; full envelope; content visible — not HTTP 200 alone)

**Copy rule:** Copy each URL as one complete line. Do not use truncated/chat-preview text. Expected share lengths: Brand AI **373**, GDI **476**. Companion plain-text file: `FINAL_URLS_V3.txt`.

---

## Final four external URLs

### 1. Development Radar — Mexico

https://my-operators-backend-production.up.railway.app/radar-share.html?pack=mexico

### 2. Brand AI Intelligence — Hilton

https://my-operators-backend-production.up.railway.app/brand-ai-visibility-share.html?share=baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfNWRlNDI4ZDdkMjg0MDU0Yjg3NTZjZjBhIiwicGFyZW50Q29tcGFueUlkIjoiaGlsdG9uIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzkxMDMzNTgwLCJleHAiOm51bGx9.y6wUBvyIeq5SeWY1ox0_r_UQN-TWpxa9beb8-S1YeIg

**Integrity:** share param length = **373** · parts = **4** (`baiparent` · `v1` · payload · signature) · tid = `sht_baip_5de428d7d284054b8756cf0a` · parent = `hilton` · production-signed

### 3. Brand Explorer — Hilton

https://my-operators-backend-production.up.railway.app/brand-explorer-share.html?pack=hilton

### 4. Group Demand Intelligence — Hilton New York Times Square

https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfMTUwYmQ2YmJhNmUzMjcxNmQzYTg0OTk5IiwiaG90ZWxJZCI6InJlYzM1ZkV4VXhDQ2xwT1A2Iiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJjYXBhYmlsaXRpZXMiOlsiQ0FOX1JFQURfQlJJRUYiLCJDQU5fUkVBRF9PUFBPUlRVTklUSUVTIiwiQ0FOX1JFQURfT1BQT1JUVU5JVFlfREVUQUlMIiwiQ0FOX1ZBTElEQVRFIiwiQ0FOX1JFQURfU1VNTUFSWSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzkxMDMzNTgwLCJleHAiOm51bGx9.EkTmRr2jiEI-tkvlXZ_xnnt7bmhCRerdT-6nal45wFo

**Integrity:** share param length = **476** · parts = **4** (`gdishare` · `v1` · payload · signature) · tid = `gdisht_150bd6bba6e32716d3a84999` · hotel = Hilton New York Times Square · production-signed

---

## Also available

### Opportunity Radar — Mexico

https://my-operators-backend-production.up.railway.app/opportunity-radar-share.html?pack=mexico

---

## V3 recovery notes (2026-10-03)

### Founder symptoms vs exact handoff URLs

| Symptom | Exact complete handoff URL | Truncated URL (signature missing) |
|---------|----------------------------|-----------------------------------|
| Brand AI `malformed_share_capability` | **Not reproduced** — resolve 200, Hilton brands + Executive Summary render | **Reproduced** — API `401` `error=malformed_share_capability` `code=SHARE_MALFORMED` |
| GDI “Access link unavailable” | **Not reproduced** — Hilton NYTS + 12 opportunities + detail drawer | Same customer message for `SHARE_MALFORMED` / bad signature / revoked |

**Root cause of founder-visible errors:** incomplete share envelope (most commonly signature segment cut off by chat/email copy). A valid token has **exactly 4** dot-separated parts. Missing the final `.sig` segment yields `malformed_share_capability`.

**Handoff integrity check (pre-V3 file):** no ellipsis (`…`), no smart quotes, no whitespace in share params. Complete URLs already verified; V3 reissues fresh production signatures for the same logical tids for clean redistribution.

### Brand AI

- Logical tid preserved: `sht_baip_5de428d7d284054b8756cf0a`
- Fresh production-signed envelope (iat `1791033580`)
- Clean browser: Hilton header, Curio/Tapestry/Canopy/Tempo selector, Executive Summary (36.7% portfolio presence), Detailed View usable
- No `malformed_share_capability` with full URL

### GDI

- Logical tid preserved: `gdisht_150bd6bba6e32716d3a84999`
- Fresh production-signed envelope (iat `1791033580`)
- Clean browser: Hilton New York Times Square, 12 opportunities, detail drawer loads opportunity body
- No “Access link unavailable” with full URL

### Tokens

| Product | Token id | Notes |
|---------|----------|--------|
| Brand AI | `sht_baip_5de428d7d284054b8756cf0a` | Logical id preserved; V3 production envelope |
| GDI | `gdisht_150bd6bba6e32716d3a84999` | Logical id preserved; V3 production envelope |

No localhost URLs. No DEV-signed tokens in this handoff.

Plain-text copy pack: `FINAL_URLS_V3.txt` · machine metadata: `FINAL_URLS_V3.json`
