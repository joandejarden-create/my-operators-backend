# Residual sourceSummary drift

Isolated after the two-write pilot comparison fix. **Do not mass-apply.**

| Opportunity | Classification | Apply? |
|-------------|----------------|--------|
| `gdi_opp_umd_alumni_weekend_watch` | AIRTABLE_SOURCE_SUMMARY_OBJECT_OBJECT_CORRUPTION | **No** |
| `gdi_opp_marriott_hq_adjacent_corporate_watch` | AIRTABLE_SOURCE_SUMMARY_OBJECT_OBJECT_CORRUPTION | **No** |
| `disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_` | FS_LAG_AFTER_AUTHORIZED_PDA_UPDATE | **No** |

## Detail

```json
[
  {
    "opportunityId": "gdi_opp_umd_alumni_weekend_watch",
    "airtableRecordId": "recQ9DEXmR3Yaw6sk",
    "materialChanged": true,
    "changedFields": [
      "sourceSummary"
    ],
    "airtableSourceSummary": "[object Object]",
    "filesystemSourceSummary": "GDI pilot research note",
    "classification": "AIRTABLE_SOURCE_SUMMARY_OBJECT_OBJECT_CORRUPTION",
    "applyRecommended": false
  },
  {
    "opportunityId": "gdi_opp_marriott_hq_adjacent_corporate_watch",
    "airtableRecordId": "recgRriItFBL2HSfC",
    "materialChanged": true,
    "changedFields": [
      "sourceSummary"
    ],
    "airtableSourceSummary": "[object Object] | [object Object]",
    "filesystemSourceSummary": "GDI pilot research note | Webhound discovery session corporate assessment",
    "classification": "AIRTABLE_SOURCE_SUMMARY_OBJECT_OBJECT_CORRUPTION",
    "applyRecommended": false
  },
  {
    "opportunityId": "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
    "airtableRecordId": "recTSOXMxDT7JpxGq",
    "materialChanged": true,
    "changedFields": [
      "sourceSummary",
      "lastMaterialChangeAt",
      "researchPayloadJson"
    ],
    "airtableSourceSummary": "historical_attestation_source (agent-transcript commerciallyApprovedStaged (PDA)) | https://www.pda.org/global-event-calendar/event-detail/design-validation-and-operation-of-isolator-systems-training-",
    "filesystemSourceSummary": "historical_attestation_source (agent-transcript commerciallyApprovedStaged (PDA)) | https://www.pda.org/global-event-calendar/event-detail/design-validation-and-operation-of-isolator-systems-training-",
    "classification": "FS_LAG_AFTER_AUTHORIZED_PDA_UPDATE",
    "applyRecommended": false
  }
]
```

Notes:
- UMD + Marriott HQ still carry Airtable `[object Object]` sourceSummary corruption from earlier writes; FS rebuilds clean strings. Separate repair authorization required.
- PDA FS bag lags the authorized Dec 2026 evidence UPDATE already in Airtable — do not overwrite Airtable with older FS.
