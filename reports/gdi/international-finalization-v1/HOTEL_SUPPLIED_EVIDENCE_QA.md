# Hotel-Supplied Response Mapping QA

| Response | evidenceType | selectionStatus | lodgingClass | closed | readyFromResponse |
|----------|--------------|-----------------|--------------|--------|-------------------|
| Please send us your rates for December | ORGANIZER_RESPONSE | UNKNOWN | UNCONFIRMED | false | false |
| Hotel list already finalized | ORGANIZER_RESPONSE | CLOSED | UNCONFIRMED | true | false |
| Delegates book individually from our recommended list | ORGANIZER_RESPONSE | UNKNOWN | UNCONFIRMED | false | false |
| Our PCO handles rooms — contact EventAgency GmbH | PCO_RESPONSE | UNKNOWN | UNCONFIRMED | false | false |
| We need overflow rooms near the venue | ORGANIZER_RESPONSE | UNKNOWN | UNCONFIRMED | false | false |

Rule: **Outreach / hotel-supplied response alone never creates Ready.**
