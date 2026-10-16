# Task-by-segment reconciliation

Allocation ran PE tasks after scaling; PE Gaslamp/yacht returned more publishable venue rows while scaling candidates were false-quarantined by geo gate (localConnection omitted from blob; DC hard-coded foreign even for Mayflower).

| Task | Hotel | Segment | Kind | Run ID | Candidates | Est USD |
|------|-------|---------|------|--------|------------|--------:|
| contact_wisedocs_may | rec0eTEnCqlsGJJt9 | scaling_companies | contact_recovery | trun_567f3335d0c140ad86f8f8a3bf3a4d74 | 1 | 0.15 |
| contact_shipyard_sd | recnPB3Q02NO8Ma1o | scaling_companies | contact_recovery | trun_567f3335d0c140ad850a77a02ae0c76f | 1 | 0.15 |
| scaling_discovery_may | rec0eTEnCqlsGJJt9 | scaling_companies | discovery | trun_567f3335d0c140ad9f9a06c90dc236ed | 2 | 0.25 |
| scaling_discovery_sd | recnPB3Q02NO8Ma1o | scaling_companies | discovery | trun_567f3335d0c140ad81932da972fcf37f | 2 | 0.25 |
| lmc_discovery_may | rec0eTEnCqlsGJJt9 | local_meetings_catering | discovery | trun_567f3335d0c140adb31371a8f4d0d258 | 6 | 0.25 |
| lmc_discovery_sd | recnPB3Q02NO8Ma1o | local_meetings_catering | discovery | trun_567f3335d0c140adbf266a5d5bfa5fd6 | 3 | 0.25 |
| pe_may_clubs_embassy | rec0eTEnCqlsGJJt9 | weddings_social_private_events | discovery | trun_567f3335d0c140ad870abd2c667565ea | 4 | 0.25 |
| pe_sd_gaslamp_yacht | recnPB3Q02NO8Ma1o | weddings_social_private_events | discovery | trun_567f3335d0c140ad9bc68e2c107a280e | 8 | 0.35 |
| scaling_discovery_may_v2 | rec0eTEnCqlsGJJt9 | scaling_companies | discovery | trun_567f3335d0c140adb82a54365648fad5 | 5 | 0.35 |
| scaling_discovery_sd_v2 | recnPB3Q02NO8Ma1o | scaling_companies | discovery | trun_567f3335d0c140ad949f162eb795ee58 | 6 | 0.35 |
| surfe_named_contact_budget | — | — | — | — | — | 1 |

### Continuation notes
- Quarantine reprocess used stored raw results only (no resubmit of above run_ids).
- v2 tasks used distinct job segments so Parallel did not resume prior scaling runs.
- Surfe on Wisedocs/Shipyard named buyers: **$0** (usable org email already present).

## Budget reconciliation

```json
{
  "authorizedUsd": 11,
  "historicalPilotUsdPreserved": 2,
  "estimatedSpentUsd": 2.85,
  "actualBilledUsd": null,
  "outstandingReservationsUsd": 0,
  "outstandingReservationCount": 0,
  "releasedCount": 0,
  "spentEntryCount": 11,
  "availableUncommittedUsd": 8.15,
  "note": "Do not assume prior $9.10 figure — recompute from shared ledger. Surfe AMWA $0.25 included in estimatedSpent.",
  "byEntry": [
    {
      "label": "paid_ms:contact_wisedocs_may",
      "hotelId": "rec0eTEnCqlsGJJt9",
      "estimatedUsd": 0.15,
      "providerRunId": "trun_567f3335d0c140ad86f8f8a3bf3a4d74",
      "status": "spent"
    },
    {
      "label": "paid_ms:contact_shipyard_sd",
      "hotelId": "recnPB3Q02NO8Ma1o",
      "estimatedUsd": 0.15,
      "providerRunId": "trun_567f3335d0c140ad850a77a02ae0c76f",
      "status": "spent"
    },
    {
      "label": "paid_ms:scaling_discovery_may",
      "hotelId": "rec0eTEnCqlsGJJt9",
      "estimatedUsd": 0.25,
      "providerRunId": "trun_567f3335d0c140ad9f9a06c90dc236ed",
      "status": "spent"
    },
    {
      "label": "paid_ms:scaling_discovery_sd",
      "hotelId": "recnPB3Q02NO8Ma1o",
      "estimatedUsd": 0.25,
      "providerRunId": "trun_567f3335d0c140ad81932da972fcf37f",
      "status": "spent"
    },
    {
      "label": "paid_ms:lmc_discovery_may",
      "hotelId": "rec0eTEnCqlsGJJt9",
      "estimatedUsd": 0.25,
      "providerRunId": "trun_567f3335d0c140adb31371a8f4d0d258",
      "status": "spent"
    },
    {
      "label": "paid_ms:lmc_discovery_sd",
      "hotelId": "recnPB3Q02NO8Ma1o",
      "estimatedUsd": 0.25,
      "providerRunId": "trun_567f3335d0c140adbf266a5d5bfa5fd6",
      "status": "spent"
    },
    {
      "label": "paid_ms:pe_may_clubs_embassy",
      "hotelId": "rec0eTEnCqlsGJJt9",
      "estimatedUsd": 0.25,
      "providerRunId": "trun_567f3335d0c140ad870abd2c667565ea",
      "status": "spent"
    },
    {
      "label": "paid_ms:pe_sd_gaslamp_yacht",
      "hotelId": "recnPB3Q02NO8Ma1o",
      "estimatedUsd": 0.35,
      "providerRunId": "trun_567f3335d0c140ad9bc68e2c107a280e",
      "status": "spent"
    },
    {
      "label": "surfe_contact_enrichment",
      "hotelId": "rec0eTEnCqlsGJJt9",
      "estimatedUsd": 0.25,
      "providerRunId": "bcb0a16b-063e-4654-8569-f87e707c9bf6",
      "status": "spent"
    },
    {
      "label": "paid_ms:scaling_discovery_may_v2",
      "hotelId": "rec0eTEnCqlsGJJt9",
      "estimatedUsd": 0.35,
      "providerRunId": "trun_567f3335d0c140adb82a54365648fad5",
      "status": "spent"
    },
    {
      "label": "paid_ms:scaling_discovery_sd_v2",
      "hotelId": "recnPB3Q02NO8Ma1o",
      "estimatedUsd": 0.35,
      "providerRunId": "trun_567f3335d0c140ad949f162eb795ee58",
      "status": "spent"
    }
  ]
}
```
