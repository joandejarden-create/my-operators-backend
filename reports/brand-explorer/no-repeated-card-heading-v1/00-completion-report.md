# BE_NO_REPEATED_CARD_HEADING_V1 — Completion Report

**Status:** BROWSER_VERIFIED (enforcement + Comparison Studio) · **NOT** founder-approved for publication  
**Date:** 2026-11-19  
**Stream:** Brand Explorer (`ws-brand-explorer`)  
**Lesson:** `BE-QL-NO-REPEATED-HEADING-001`  
**Curio / Ascend:** **NOT APPROVED**  
**Airtable writes:** 0  
**Gate 2:** Still **BLOCKED** until Gate 1 founder approval

## Founder failure

Card titles were repeated at the start of descriptions:

- Title: `Collection Scale Context`
- Body: `Collection Scale Context: Choice Hotels positions Ascend…`

Two causes:

1. Stored presentation bodies already echoed the title with a colon.
2. Atelier `explorerMergedBody` prepended `title + ': ' + body` whenever both existed, even when the card already rendered the title as a white heading.

## Fix

| Layer | Change |
| --- | --- |
| Utility | `lib/brand-explorer/be-no-repeated-card-heading-v1.js` — strip leading title echo (case/punct insensitive); preserve later in-body mentions |
| Browser mirror | `public/js/brand-explorer-no-repeated-card-heading.js` |
| Assembler | Cumulative RIGHT proposal applies strip across titled slots |
| Renderer defense | `explorerMergedBody` / proof cards use body-only + `stripCardHeadingEcho` |
| Fixture | `fixtures/brand-explorer-no-repeated-card-heading/founder-title-echo-examples.json` |
| Contract | `configs/brand-explorer-permanent-content-standards-contract-v1.json` → `typographyAndWriting.cardHeadings.forbidRepeatedTitleInBody` |
| Gate | `npm run test:be-no-repeated-card-heading-v1` |
| Lesson registry | `BROWSER_VERIFIED` with screenshot paths |

## Automated gate

```json
{
  "ok": true,
  "contract": "BE_NO_REPEATED_CARD_HEADING_V1",
  "fixtureFailuresBefore": 3,
  "headingEchoChangedOnProposed": 0,
  "ascendProofBody": "Choice Hotels positions Ascend as an upscale soft collection for distinctive conversions.",
  "airtableWrites": 0,
  "founderApproved": false
}
```

Ascend Collection Scale Context body no longer starts with the title echo.

## Browser evidence (Comparison Studio)

URL: `http://127.0.0.1:8082/brand-explorer-compare.html?beInternalPreview=1&brand=receQkxgjlezsc1xg`

| Shot | Observation |
| --- | --- |
| `screenshots/01-curio-proof-no-repeated-heading-RIGHT.png` | RIGHT proof titles Title Case; bodies start with substance (no `Title:` prefix) |
| `screenshots/02-curio-dealality-view-no-title-echo.png` | RIGHT Dealality View body starts `Curio reads as…` — not `Dealality View on Market Presence:` |

LEFT CURRENT remains the saved profile; title-echo defense applies at render for both mounts where titles are already shown as headings.

## Authorization boundaries

- No Airtable writes
- No paid research
- No publication
- No automatic founder approval
- Curio / Ascend remain **NOT APPROVED**
