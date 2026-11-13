# GDI — Remove Market Scope pills from opportunity cards

**Branch:** `fix/gdi-regression-recovery-v1`  
**Date:** 2026-11-13  
**Scope:** Presentation only (`dealality-gdi-ui.js`)

## Change

`competitiveStatusPillHtml` no longer appends competitive commercial-action labels (`Qualify Overflow`, `Monitor Future Cycle`, etc.) as card pills. Cards keep **Verified / Needs Verification / Historical** only for CMI status.

Market Scope remains the toolbar (All / Hotel / Competitive). Drawer still shows Qualify Overflow and competitor evidence.

## Verified

- Bethesda Competitive (10): ACVNU card pills = High · Verified · Qualify Opportunity · date — no Qualify Overflow on card
- Drawer: ACVNU still opens with Qualify Overflow · Verified banner + Confirmed Competitor Business
- Tests: `test:gdi-competitive-future-cycle-presentation-v1`, `test:gdi-market-scope-toggle-v1`, `test:gdi-regression-recovery-v1`
