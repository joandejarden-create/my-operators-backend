# GDI — Remove verification status labels from opportunity cards

**Branch:** `fix/gdi-regression-recovery-v1`  
**Date:** 2026-11-13  
**Scope:** Presentation only (`dealality-gdi-ui.js`) — not deployed

## Change

Opportunity cards no longer render Verified / Needs Verification / Historical badges.

Verification remains in the Opportunity Details drawer (e.g. `Qualify Overflow · Verified`) and in underlying data / Market Scope / CSV.

Card pills retained: Priority · sales action · event date.

## Verified (Bethesda)

- All 67 / Hotel 61 / Competitive 10
- Competitive cards: e.g. `High · Qualify Opportunity · Apr 11–17, 2027` — no Verified pill
- Drawer ACVNU: verification + Qualify Overflow still present
- Tests: competitive future-cycle, market-scope toggle, regression recovery — PASS
