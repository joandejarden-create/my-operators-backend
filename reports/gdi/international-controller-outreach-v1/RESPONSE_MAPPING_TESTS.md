# Synthetic response mapping tests (TEST fixtures only)

Pass: **YES**

| Fixture | Expected | Got | Ready from reply? | Prod persist blocked? | Pass |
|---------|----------|-----|-------------------|-----------------------|------|
| TEST_OPEN | RATE_REQUESTED | RATE_REQUESTED | false | true | YES |
| TEST_CLOSED | HOTEL_LIST_FINALIZED | HOTEL_LIST_FINALIZED | false | true | YES |
| TEST_PCO_REDIRECT | PCO_REDIRECT | PCO_REDIRECT | false | true | YES |
| TEST_RATE_REQUESTED | RATE_REQUESTED | RATE_REQUESTED | false | true | YES |
| TEST_SELF_BOOKING_ONLY | SELF_BOOKING_ONLY | SELF_BOOKING_ONLY | false | true | YES |
| TEST_CAN_APPLY | TARGET_HOTEL_CAN_APPLY | TARGET_HOTEL_CAN_APPLY | false | true | YES |

Synthetic fixtures only — do not persist as HOTEL_SUPPLIED_EVIDENCE production records
