# Data population quality

| Area | Present? | Populated? | Accurate? | Full E2E populates? | Survives persistence? | Notes |
|------|----------|------------|-----------|---------------------|------------------------|-------|
| opportunityId/hotelId | Y | Y | Y | When synced | Y | Registry spine |
| venueStatus/sourcingStatus | Y | Partial | Mixed aliases | Partial | Typed when written | Enum alias cleanup |
| peakRooms/roomDemandStatus | Y | Partial | Claim-dependent | Partial | Y | Don’t invent from attendance |
| primary contact typed | Y | Partial | Role overload | Partial | Y | Title vs role in JSON |
| lastMaterialChange* | Mapped | Sparse | — | Rarely | Often payload-only | Populate for Weekly Change |
| salesActionKey | JSON | Often compacted | Resolver OK | Presentation path | Weak on Airtable-only rebuild | Non-compaction P0 |
| CMI competitor* | JSON | Partial | Contract gated | CMI hunter | Publication freeze | Keep JSON |
| weeklyDeltaState | Derived/FS | Inconsistent | Engine OK | Weekly refresh | Not typed | Persist annotation |
| DG links on GDO | Proposed | No | — | Not yet | — | CD-3 |
| Feedback outcomes | Decisions | Active path | Certified | Feedback only | Y | Not Full E2E |

Representative hotels (FS vs AT drift) reinforce that **population and identity reconciliation** dominate over missing columns — see persistence adapter multi-hotel reconciliations.
