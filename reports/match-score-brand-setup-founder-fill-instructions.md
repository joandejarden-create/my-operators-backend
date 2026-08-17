# Match Score Brand Setup — Founder fill instructions (P1)

## Goal

Make Match Score **trustworthy when Brand Setup is complete**. Do **not** invent values to raise scores.

## Universe

Live Active/Live count is in `reports/match-score-active-live-brand-list.json` (currently **24**, not 26).

## Sources only

| Code | Meaning |
|------|---------|
| **A** | Founder / brand knowledge |
| **B** | Existing Dealality docs / FDD / prior research in repo or Drive |

Forbidden: web/AI invent-and-write; Explorer marketing copy as fees/geography; defaulting Priority Markets to Global.

## Workflow

1. Open `reports/match-score-brand-setup-founder-worksheet.csv` (or `.json`).
2. For each blank **required** row, set `proposedValue`, `sourceType` (`A` or `B`), `sourceRef`, `filledBy`, `filledDate`.
3. Complex fields:
   - `roomCountRange`: JSON `{"min":80,"max":200}`
   - `feeRoyaltyRange`: JSON `{"min":4,"max":6,"basis":"% of rooms revenue"}`
   - `feeMarketingOrLoyalty`: JSON `{"marketingMin":2,"marketingMax":3,"loyaltyMin":...,"loyaltyMax":...}`
   - Multi-selects: comma- or `\|`-separated values, or JSON array
4. Dry-run: `npm run apply-match-score-brand-setup-fills -- --dry-run`
5. Apply after approve: `npm run apply-match-score-brand-setup-fills -- --apply`
6. Re-audit: `npm run audit-match-score-brand-setup-gaps` until P1 complete = Active/Live count
7. Refresh scores: `npm run refresh-deal-brand-cache-active-brands -- --apply`

## P2

See `reports/match-score-brand-setup-p2-full-completeness-backlog.md` for full Brand Setup tab completeness after P1.
