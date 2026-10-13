# Placement Gate Forensics — 2026-10-03

## Classes observed (subjects)

### YOTEL
Classes: {"UNKNOWN":13}
Rejected for placement: 0
(none)

### SPICE
Classes: {"UNKNOWN":76,"OPEN_PATH":1}
Rejected for placement: 0
(none)

### AC
Classes: {"UNKNOWN":31,"OPEN_PATH":1}
Rejected for placement: 0
(none)

## Assessment

Funnel placement pass treats UNKNOWN as allowed (does not kill). Explicit FULLY_PLACED / PRIMARY_NO_OVERFLOW kill.

**Is primary venue selected incorrectly treated as no hotel opportunity?**

In `hasHotelOpportunityThesis`, a named competitor/host hotel in `venueStatus` **helps** thesis (overflow pursue motion) — it does **not** auto-DQ. Separate commercial status PRIMARY_NO_OVERFLOW can kill.

On subject hotels, placement rejects are **rare** relative to lodging/entity/geography. Placement is **not** the dominant cross-hotel kill gate.

Placement logic blocking valid opportunities at scale: **NO**.
