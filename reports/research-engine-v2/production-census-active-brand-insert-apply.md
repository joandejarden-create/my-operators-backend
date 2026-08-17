# Active Brand Setup Insert Apply

**Status:** `production_census_active_brand_insert_apply_partial_steward_remaining`

Selected run: `reports/research-engine-v2/autopilot/2026-08-05T23-52-53_CALA-source-discovery`

## Preflight

- Bundle: 91
- Pass: **75**
- Steward remaining: **16** (Choice "a member of Radisson Individuals" names)
- Duplicate risk: 0

## Apply

- Created: **75**
- Airtable writes: true
- Target: Hotel Property Census (tbl9aY5ijiuIzzWam)
- Census count after: **741** (before rededupe index ~666; delta ~75)
- Brand Setup / Brand Explorer / VIC / old Census: untouched

## Validation

- Created count matches preflight pass: true
- Sample Production Use Status / forbidden fields:
  - ind_hilton_do_pujoydt: production=Census Only / Not Owner-Facing, owner=∅, CV=∅, BV=∅
  - ind_choice_cr_cr013: production=Census Only / Not Owner-Facing, owner=∅, CV=∅, BV=∅
  - ind_marriott_cr_sjoaa: production=Census Only / Not Owner-Facing, owner=∅, CV=∅, BV=∅
  - ind_choice_co_cb008: production=Census Only / Not Owner-Facing, owner=∅, CV=∅, BV=∅
  - ind_marriott_do_stiac: production=Census Only / Not Owner-Facing, owner=∅, CV=∅, BV=∅

## Next

Steward 16 Choice Radisson Individuals names with member-of suffixes + unknown city; optional name normalize then re-queue
