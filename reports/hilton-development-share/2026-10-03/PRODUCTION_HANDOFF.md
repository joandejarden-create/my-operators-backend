# Hilton Development — Production External Share Handoff

**Date:** 2026-10-03  
**Environment:** production  
**Production host:** https://my-operators-backend-production.up.railway.app  
**Production SHA:** `3544ed38d162f3b7bedf8c397e56f047644aeab6`  
**Railway deployment:** `732672e9-b3dd-4d41-b989-c0639af50f5e` (SUCCESS)  
**Branch:** `hotfix/opportunity-radar-share` (from Hilton share base `0ad4c8a`)

---

## External URLs

### Opportunity Radar — Mexico

https://my-operators-backend-production.up.railway.app/opportunity-radar-share.html?pack=mexico

**Description:** Shows development opportunities, market signals and prioritized targets from Dealality's Opportunity Radar in a read-only external view.

This is the actual Opportunity Radar (`/app#/opportunity-radar` → `deal-capture-radar-with-ranked-list.html`), **not** Scout Market Map (`/radar-share.html`).

### Prior Hilton Development shares (still live)

1. **Scout Market Map — Mexico**  
   https://my-operators-backend-production.up.railway.app/radar-share.html?pack=mexico

2. **Brand AI Intelligence — Hilton** (V3 production envelope — shareLen=373)  
   https://my-operators-backend-production.up.railway.app/brand-ai-visibility-share.html?share=baiparent.v1.eyJ2IjoxLCJraW5kIjoiQkFJX1BBUkVOVF9DT01QQU5ZX1NIQVJFIiwidGlkIjoic2h0X2JhaXBfNWRlNDI4ZDdkMjg0MDU0Yjg3NTZjZjBhIiwicGFyZW50Q29tcGFueUlkIjoiaGlsdG9uIiwic3VyZmFjZXMiOlsicmVwb3J0IiwiZXZpZGVuY2UiLCJwb3J0Zm9saW8iLCJleGVjdXRpdmVfc3VtbWFyeSJdLCJyZXBvcnRTY29wZSI6ImN1cnJlbnRfcHVibGlzaGVkIiwiaWF0IjoxNzkxMDMzNTgwLCJleHAiOm51bGx9.y6wUBvyIeq5SeWY1ox0_r_UQN-TWpxa9beb8-S1YeIg

3. **Brand Explorer — Hilton**  
   https://my-operators-backend-production.up.railway.app/brand-explorer-share.html?pack=hilton

4. **Group Demand Intelligence — Hilton New York Times Square** (V3 production envelope — shareLen=476)  
   https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfMTUwYmQ2YmJhNmUzMjcxNmQzYTg0OTk5IiwiaG90ZWxJZCI6InJlYzM1ZkV4VXhDQ2xwT1A2Iiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJjYXBhYmlsaXRpZXMiOlsiQ0FOX1JFQURfQlJJRUYiLCJDQU5fUkVBRF9PUFBPUlRVTklUSUVTIiwiQ0FOX1JFQURfT1BQT1JUVU5JVFlfREVUQUlMIiwiQ0FOX1ZBTElEQVRFIiwiQ0FOX1JFQURfU1VNTUFSWSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzkxMDMzNTgwLCJleHAiOm51bGx9.EkTmRr2jiEI-tkvlXZ_xnnt7bmhCRerdT-6nal45wFo

---

## Opportunity Radar share — how it works

| Item | Value |
|------|--------|
| Auth type | Pack share (`?pack=mexico&share=1`) — no login, no signed token V1 |
| Internal source | `/app#/opportunity-radar` → `deal-capture-radar-with-ranked-list.html` + `brand-presence-mapping.js` |
| Data API | `GET /api/brand-presence?view=map&country=Mexico` (same census map DTO) |
| Mexico lock | Country filter disabled + server-side `country=` filter |
| Writes | Client fetch guard rejects POST/PUT/PATCH/DELETE in share mode |

No localhost URLs. No DEV-signed envelopes. Global Memberstack auth unchanged.
