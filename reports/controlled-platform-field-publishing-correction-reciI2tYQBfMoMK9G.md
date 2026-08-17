# GHL specificMarkets — Steward Correction Plan

Generated: 2026-07-06T15:33:50.814Z

## 1. Entity

- **GHL Hoteles (GHL Holding)**
- Operator record: `reciI2tYQBfMoMK9G`

## 2. Destination

- Table: **Operator Setup - Platform & Markets**
- Field: `specificMarkets`
- Destination record: `recbY3IGCh2LZQ3Mi`

## 3. Current live value

`Latin America, Colombia, Peru, Chile, Guatemala`

## 4. Recommended corrected value

**Colombia, Chile, Guatemala, Peru**

## 5. Reason for correction

Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context.

The prior controlled publish used the events-page source, which included regional framing. The official destinations page lists four specific countries.

## 6. Evidence source

- `reckrUB2WmnSm02g3` — GHL Hoteles destinations
- https://www.ghlhoteles.com/en/destinations/
- Destinations listed: Colombia, Chile, Guatemala, Peru

## 7. Latin America as context only

"Latin America" appears on the official destinations page as regional framing ("Destinations in Latin America") but `specificMarkets` should store the four country names only. Keep regional scope in PI evidence and fact context — not in this platform field unless the field definition is explicitly regional.

## 8. Not a governance correction

This updates only `specificMarkets` on Platform & Markets. Validation Status, external display, and profile governance are unchanged.

## 9. Company Validated untouched

Company Validated and Company Validation Date are not read or written by this correction path.

## 10. Steward approval required

Do not apply without reviewing this plan and the correction dry-run report. Use `--approve-controlled-field-correction` only after explicit approval.

## Prior controlled publish

- Fact: `reccszsLnWjA5fPnp` (`op.markets.regionsSupported`)
- Original source: `recoOcRjSD3VZb3qt` — GHL Hoteles events
- Prior value: `Latin America, Colombia, Peru, Chile, Guatemala`

## Commands

**Dry-run (run now):**

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --destination-field specificMarkets --correct-value "Colombia, Chile, Guatemala, Peru" --reason "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context." --dry-run
```

**Apply (founder approval only — do not run without sign-off):**

```bash
npm run controlled-platform-field-publishing -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --destination-field specificMarkets --correct-value "Colombia, Chile, Guatemala, Peru" --reason "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context." --apply --approve-controlled-field-correction
```

Run apply only after founder/steward approves dry-run correction plan.


## Correction dry-run result

- Eligible: **yes**
- Planned: `Latin America, Colombia, Peru, Chile, Guatemala` → **Colombia, Chile, Guatemala, Peru**

---

# Controlled Platform Field Publishing — Steward Correction

Generated: 2026-07-06T15:33:50.813Z
Version: **v2** (correction mode)
Mode: **correction-dry-run**

## Entity

- **GHL Hoteles (GHL Holding)** (`reciI2tYQBfMoMK9G`)
- Type: operator

## Destination

- Table: **Operator Setup - Platform & Markets**
- Field: `specificMarkets`
- Record: `recbY3IGCh2LZQ3Mi`

## Values

- **Current live:** Latin America, Colombia, Peru, Chile, Guatemala
- **Recommended corrected:** Colombia, Chile, Guatemala, Peru
- **Reason:** Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context.

## Evidence

- Source: `reckrUB2WmnSm02g3` — GHL Hoteles destinations
- URL: https://www.ghlhoteles.com/en/destinations/

## Regional context vs specificMarkets

"Latin America" appears on the official destinations page as regional framing ("Destinations in Latin America") but `specificMarkets` should store the four country names only. Keep regional scope in PI evidence and fact context — not in this platform field unless the field definition is explicitly regional.

## Correction plan

- Previous: Latin America, Colombia, Peru, Chile, Guatemala
- New: **Colombia, Chile, Guatemala, Peru**

## Safety confirmations

- Not a governance correction
- Company Validated / Company Validation Date untouched
- PI facts, sources, and scoring unchanged
- Requires explicit `--approve-controlled-field-correction` before apply

## Rollback

- Restore `specificMarkets` to: Latin America, Colombia, Peru, Chile, Guatemala
