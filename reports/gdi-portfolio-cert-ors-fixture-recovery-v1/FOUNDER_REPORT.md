# GDI Portfolio Cert ORS Fixture Recovery V1 — Founder Report

**Date:** 2026-11-23  
**Base:** `f33e6670b2bae3930aa168197c2b9210a7f2ab88`  
**Authorization:** $0 REMEDIATION — no paid research / Airtable writes / customer publication

## Root cause

`.gitignore` commit `d719eca7a` intentionally ignores:

`data/group-demand-intelligence/hotels/*/opportunity-research-state/`

V1.1–V1.3 PRs therefore could not commit the eight live ORS JSON files. Offline V1.2/V1.3 tests required those files on disk and failed on fresh checkouts.

## Remedy

Committed curated fixtures under:

`fixtures/gdi-portfolio-cert-v1-1-frozen-ors-fixtures-v1/`

`resolveV11OrsPaths` now prefers these fixtures, then falls back to local runtime ORS.

Qualification standards unchanged (8 EXCLUDE → 8 WATCHLIST_RESEARCH, 0 Sales Ready, 0 publication, 0 URL improvements).
