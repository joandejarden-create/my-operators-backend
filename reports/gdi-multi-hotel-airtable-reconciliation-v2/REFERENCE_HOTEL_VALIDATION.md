# Reference hotel validation

Eight hotels reconciled at opportunity-ID grain. Full matrix: `HOTEL_INVENTORY_RECONCILIATION_MATRIX.md`. JSON rows: `*_ID_LEVEL_RECONCILIATION.json`.

## Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)

- FS 82, AT 82, overlap 82
- **82 UNCHANGED** — reference bar for idempotent FS↔Airtable alignment

## Castillo Hotel Son Vida (`rec82D9zpB8fede1I`)

- FS 20, AT 20, overlap 20
- **20 UNCHANGED**

## Bethesda Marriott (`recLuxvwwxID7U2B8`)

- FS 59, AT 86, overlap 59
- **57 UNCHANGED**, **1 UPDATE**, **1 CONFLICT**, **27 DEFERRED** (AT-only historical)
- UPDATE: `gdi_opp_accp_annual_meeting_20261001` — priority HIGH → DISQUALIFIED (filesystem commercial state)
- CONFLICT: `gdi_opp_series_ctn_annual_conference_2027` — org name wording (NIDA Clinical Trials Network vs abbreviated CTN label)

## Waterstone Resort & Marina (`recgMYovrrZDJMqzX`)

- FS 29, AT 36, overlap 29
- **29 UNCHANGED**, **7 DEFERRED** (AT-only — preserve history)

## Nobu Hotel London Portman Square (`recC2kbCaS48jFjFF`)

- FS **0**, AT **65**
- Local opportunities bag absent on filesystem; **65 DEFERRED** (Airtable historical registry — no silent import)

## Cross-reference hotels (PRIMARY)

YOTEL, Radisson, Guadalajara detailed in dedicated `*_ID_LEVEL_RECONCILIATION.md` files.

## Validation outcome

Reference cohort confirms: perfect alignment possible (Wyndham/Castillo), expected AT-only history (Nobu/Waterstone/Bethesda), and bounded update/conflict handling without bulk CREATE.