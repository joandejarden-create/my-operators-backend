# ADP + GDI HPC migration

Generated: 2026-09-17T16:22:13.394Z
Mode: dry-run

## Creates / reuses

```json
{
  "created": [],
  "reused": [
    {
      "identityKey": "ind_hilton_us_bocar",
      "id": "recgMYovrrZDJMqzX"
    },
    {
      "identityKey": "ind_marriott_us_nycrn",
      "id": "recG66DQJKP2c0UNh"
    },
    {
      "identityKey": "ind_hyatt_us_nycnh",
      "id": "recGkME49yYuxQl0u"
    },
    {
      "identityKey": "ind_hilton_us_mkccuqq",
      "id": "rec8hHupaSwiWI3r7"
    }
  ],
  "wouldCreate": [],
  "duplicatesPrevented": [],
  "createdMap": {
    "recLuxvwwxID7U2B8": "recLuxvwwxID7U2B8",
    "gdi_hotel_waterstone_boca_raton": "recgMYovrrZDJMqzX",
    "gdi_hotel_renaissance_times_square": "recG66DQJKP2c0UNh",
    "adp_now_now_noho": "recGkME49yYuxQl0u",
    "adp_hotel_phillips_kansas_city": "rec8hHupaSwiWI3r7",
    "adp_waterstone_boca_raton": "recgMYovrrZDJMqzX",
    "adp_renaissance_times_square": "recG66DQJKP2c0UNh",
    "adp_cambridge_beaches_bermuda": "recIwaP1etgx2g9nA",
    "adp_jw_marriott_monterrey_valle": "recsn3BUKJ9PNfeZW",
    "adp_westin_monterrey_valle": "recD17Kxn6BcJjGFh",
    "adp_st_regis_mexico_city": "recRXmrakhSAuctwz",
    "adp_st_regis_cap_cana": "recN76iEE6yAaPh8H",
    "adp_jw_marriott_santo_domingo": "recESHsNsWUFYZrxR",
    "adp_radisson_santo_domingo": "recUOyzOXn2Zdp98I",
    "adp_hotel_caribe_faranda_grand": "recCEpdskZeUBvQwG",
    "adp_faranda_collection_bogota": "rec9Tp0WBb2uk6w3u",
    "adp_bethesda_marriott": "recLuxvwwxID7U2B8",
    "adp_casas_del_xvi": "recjDsNzu93CFfe87"
  }
}
```

## ID migrations

```json
{
  "skipped": true
}
```

## Config / FS

```json
{
  "configMigrations": [
    {
      "status": "canonical_already_complete",
      "newPath": "C:\\Dev\\deal-capture-proxy\\config\\group-demand-intelligence\\hotels\\recgMYovrrZDJMqzX.json",
      "hotelId": "recgMYovrrZDJMqzX"
    },
    {
      "status": "canonical_already_complete",
      "newPath": "C:\\Dev\\deal-capture-proxy\\config\\group-demand-intelligence\\hotels\\recG66DQJKP2c0UNh.json",
      "hotelId": "recG66DQJKP2c0UNh"
    }
  ],
  "fsMigrations": [
    {
      "status": "would_move",
      "from": "C:\\Dev\\deal-capture-proxy\\data\\group-demand-intelligence\\hotels\\gdi_hotel_waterstone_boca_raton",
      "to": "C:\\Dev\\deal-capture-proxy\\data\\group-demand-intelligence\\hotels\\recgMYovrrZDJMqzX"
    },
    {
      "status": "would_move",
      "from": "C:\\Dev\\deal-capture-proxy\\data\\decision-outcomes\\hotels\\gdi_hotel_waterstone_boca_raton",
      "to": "C:\\Dev\\deal-capture-proxy\\data\\decision-outcomes\\hotels\\recgMYovrrZDJMqzX"
    },
    {
      "status": "would_move",
      "from": "C:\\Dev\\deal-capture-proxy\\data\\group-demand-intelligence\\hotels\\gdi_hotel_renaissance_times_square",
      "to": "C:\\Dev\\deal-capture-proxy\\data\\group-demand-intelligence\\hotels\\recG66DQJKP2c0UNh"
    },
    {
      "status": "would_move",
      "from": "C:\\Dev\\deal-capture-proxy\\data\\decision-outcomes\\hotels\\gdi_hotel_renaissance_times_square",
      "to": "C:\\Dev\\deal-capture-proxy\\data\\decision-outcomes\\hotels\\recG66DQJKP2c0UNh"
    }
  ]
}
```

## Decision linkage

```json
{
  "skipped": true
}
```

OLD BASE (Decision/Outcome legacy): `appvtnDurnMSjINP6` — not used for new writes.
NEW CANONICAL intelligence base: `appa2cE7FTRmIbB32`.
HPC base: Deal Capture Platform via `AIRTABLE_BASE_ID_ALT`.
