# GDI Security V1 — Strix / Attack Surface

## Strix tooling status

`npm run strix:check` failed: `security/strix/targets.local.txt` missing. Docker/LLM Strix profile not runnable in this workspace state.

**Fallback:** bounded live **read-only** security probe (`scripts/gdi-security-v1-probe.mjs`) + static hardening (CSV injection, URL scheme allowlist) + existing share durability tests.

## Probe results (production share)

| Test | Expected | Result |
| --- | --- | --- |
| Valid Bethesda resolve | 200 | PASS |
| Bethesda token + Waterstone hotelId (list) | 403 | PASS |
| Bethesda token + foreign hotel detail | 403 | PASS |
| Tampered hotelId (bad signature) | ≥400 | PASS (403) |
| Malformed token | ≥400 | PASS (403) |
| Foreign hotel export CSV | 403 | PASS |
| Waterstone valid list | 200 | PASS |

**CRITICAL FAIL:** 0 · **HIGH FAIL:** 0 · **PASS:** 7

## Issues

### CSV formula injection — MEDIUM — FIXED

- **Surface:** CSV export cells  
- **Root cause:** cells starting with `=+-@` not neutralized  
- **Fix:** prefix `'` in `rowsToCsv`  
- **Test:** `test-gdi-performance-security-v1.mjs`  
- **Status:** FIXED  

### Source URL javascript: scheme — MEDIUM — FIXED

- **Surface:** detail sources HTML  
- **Root cause:** `href` used `esc()` only (HTML escape ≠ URL scheme allowlist)  
- **Fix:** `isSafeHttpUrl` allow http/https only  
- **Status:** FIXED  

### Share token / IDOR — controls confirm PASS

- Signature validation, hotel scope, capability checks fail closed (probe).  
- Status: PASS (no defect)

## Headers / CSP

Not changed this cycle (risk of breaking production). Recommend follow-up audit of CSP compatibility with GDI share pages.

## SSRF

GDI browse/detail do not server-fetch opportunity source URLs on read path. No new SSRF introduced.
