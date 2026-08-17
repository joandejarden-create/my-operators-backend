# Deal Terms — Select Option Proposals

**Status:** proposals only — do **not** add to Airtable Meta or Brand Setup HTML until founder approval, then update every required surface.

Generated: 2026-07-23T18:30:46.153Z

## Observed Meta ↔ form diffs

### Quantity - Typical Renewal Notice Period
- Form options: Year(s) | Month(s) | Day(s)
- Meta choices: Year(s) | Month(s)
- **Propose adding to Meta:** `Day(s)`

### Renewal Structure
- Form options: Renewal by Mutual Agreement Only | Automatic Renewal | Owner Option to Renew | Manager Option to Renew
- Meta choices: Other | Auto Renewal | Renewal by Mutual Agreement Only | Owner Option to Renew | Manager Option to Renew | No automatic renewal — re-licensing may be offered
- **Propose adding to Meta:** `Automatic Renewal`
- **Propose adding to Brand Setup HTML (and mirrors):** `Other`, `Auto Renewal`, `No automatic renewal — re-licensing may be offered`

### Performance Test Requirement
- Form options: Yes | No
- Meta choices: Yes
- **Propose adding to Meta:** `No`

### Duration - Typical Cure Period for Performance Test Failure
- Form options: Year(s) | Month(s) | Day(s)
- Meta choices: Month(s) | Year(s)
- **Propose adding to Meta:** `Day(s)`

### Typical Termination Fee Structure (if any)
- Form options: No Early Termination | Allowed With X Months Fees | Allowed With Step-Down Schedule | Case-by-Case | Typically None
- Meta choices: (none / missing field)
- **Propose adding to Meta:** `No Early Termination`, `Allowed With X Months Fees`, `Allowed With Step-Down Schedule`, `Case-by-Case`, `Typically None`

### Who Can Exercise Termination Right After Failed Test?
- Form options: Owner Only | Mutual | Rarely Exercised / Case-by-Case
- Meta choices: (none / missing field)
- **Propose adding to Meta:** `Owner Only`, `Mutual`, `Rarely Exercised / Case-by-Case`

## Termination fields missing from Deal Terms table

These columns are mapped in `DEAL_TERMS_FORM_TO_AIRTABLE` / Brand Setup form but **do not exist** on Airtable `Brand Setup - Deal Terms` (writes are skipped). They may already live on **Fee Structure** — do not invent Deal Terms columns without approval.

- `Typical Termination Fee Structure (if any)`
- `Typical Termination Fee Structure (if any) Text`
- `Who Can Exercise Termination Right After Failed Test?`

## Suggested new options (not present in form today)

These map real franchise language better than current choices. **Do not add** until approved.

| Field | Proposed option | Why | Interim fill (existing option) |
|-------|-----------------|-----|--------------------------------|
| Renewal Structure | `No contractual renewal / re-license only` | Matches Kimpton/IHG/Hilton FDD Item 17 (no automatic renewal; re-licensing) | `Renewal by Mutual Agreement Only` + nuance in Typical Renewal Conditions |
| Who Can Exercise Termination Right After Failed Test? | `Either party (with cure)` | Common FDD framing | `Mutual` |
| Typical Termination Fee Structure (if any) | `Liquidated damages / lost future fees` | Common franchise exit economics | `Allowed With X Months Fees` + notes text |

## Surfaces to update if any proposal is approved

1. Airtable Meta — `Brand Setup - Deal Terms` single-select choices
2. [`public/brand-setup.html`](../public/brand-setup.html) Deal Terms / Fee Structure selects
3. Operator intake mirrors if they share the same option lists
4. Writers: `scripts/lib/deal-terms-field-contract.mjs`, Kimpton/Choice/Active-Live apply profiles
5. Tests / dry-run fixtures that assert option strings
