# Pursuit data model

**Store:** `data/group-demand-intelligence/hotels/{hotelId}/pursuits.json`  
**Modules:** `lib/group-demand-intelligence/pursuit/*`  
**API:** `api/gdi-pursuit.js`

## Entity fields

| Field | Purpose |
|-------|---------|
| pursuitId | Canonical id |
| hotelId / opportunityId / accountId | Links |
| pursuitStatus | Sales workflow state |
| outreachReadiness | Copied from intelligence (read-only mirror) |
| hotelInclusionStatus | Hotel-selection track |
| responseStatus | Outreach response |
| assignedTo | Default `UNASSIGNED` |
| contact* | Name / role / org / path / email |
| firstContactDate / lastContactDate / nextFollowUpDate | Timeline |
| nextAction / nextActionReason / nextActionDueDate | Action engine |
| selectionProcess / decisionWindow* / nextTrigger | Lodging-decision bridge |
| draftSubject / draftMessage / draftLanguage / messageStatus | Draft storage (no auto-send) |
| notes / outcome / outcomeDate | CRM notes |
| hotelSuppliedEvidence[] | Provenance = `HOTEL_SUPPLIED_EVIDENCE` |
| audit[] | Field-level change trail |

## Independence rule

Pursuit updates **must not** mutate Ready / Watch / packetQuality / hotelMotionClass / travelingEntityProven.
