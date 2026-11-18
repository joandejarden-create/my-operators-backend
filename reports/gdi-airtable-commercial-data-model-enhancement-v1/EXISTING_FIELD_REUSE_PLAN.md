# Existing field reuse plan (Groups A/B)

## GROUP A — improve population

- Write `firstDiscoveredAt` / `lastMaterialChangeAt` (+ run ids) on refresh  
- Keep `venueStatus` / `sourcingStatus` / `peakRooms` / contact typed fields populated  
- Stop compacting `salesActionKey`, `salesActionPillLabel`, CMI keys, `revenueStreamClass` from payload  
- Persist `weeklyDeltaState` on FS opportunity bags

## GROUP B — links / JSON integration

- Persist `bookingWindowStatus`, `eligibilityGaps`, `contactClass`, `estimatedRoomNights` in payload  
- Wire GDO→DG text ids when CD-3 approved  
- Join need-date matches via Match Key (already corrected in contracts)  
- Serve feedback from Decisions/Events + FS merge (existing)

## Do not invent parallel systems for Weekly Change, Contacts, or feedback.
