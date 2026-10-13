# Pursuit status model

```
NOT_STARTED
PREPARE
OUTREACH_READY
CONTACTED
FOLLOW_UP_DUE
ENGAGED
IN_HOTEL_SELECTION
REQUESTED_INFORMATION
PROPOSAL_REQUESTED
HOTEL_INCLUDED
NOT_SELECTED
NO_RESPONSE
DEFERRED
CLOSED_WON
CLOSED_LOST
CLOSED_NO_ACTION
```

## Initial mapping from outreach readiness

| outreachReadiness | Initial pursuitStatus |
|-------------------|----------------------|
| OUTREACH_NOW | OUTREACH_READY |
| OUTREACH_PREPARE | PREPARE |

## Follow-up due engine

If `nextFollowUpDate <= today` and pursuit not closed → status becomes `FOLLOW_UP_DUE`.

## Suggested transitions (never auto-Ready)

| Event | Suggested status |
|-------|------------------|
| Outreach logged | CONTACTED |
| Response received | ENGAGED |
| Asked for rates/info | REQUESTED_INFORMATION |
| Inclusion confirmed | HOTEL_INCLUDED |
