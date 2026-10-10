# WHO / Contact Gate Forensics — 2026-10-03

## Current rule (unchanged)

`isGdiCustomerOpportunityReady` requires `whoResearchAttempted(opp)` — i.e. pathClass ≠ NOT_RESEARCHED.

Accepted paths (`classifyWhoHowPath`):

1. NAMED_DIRECT (name + email/phone)
2. NAMED_PARTIAL (name only)
3. FUNCTIONAL (functionalContactEmail / grade tier)
4. ORG_PATH (organizationContactUrl / officialContactPath / …)
5. NO_CONTACT_AFTER_RESEARCH (contactResearchAttempted or PUBLIC_DATA_CEILING stamp)

**Named person is NOT mandatory** for readiness. Organizer/contact route is valid if stamped.

## Counts

### YOTEL (n=13)
- WHO_ENTITY_RESOLVED: 13
- WHO_ROLE_RESOLVED: 2
- WHO_PERSON_RESOLVED: 1
- PUBLIC_CONTACT_PATH_AVAILABLE: 2
- NOT_RESEARCHED: 11
- pathClasses: {"NOT_RESEARCHED":11,"NAMED_DIRECT":1,"FUNCTIONAL":1}

### SPICE (n=77)
- WHO_ENTITY_RESOLVED: 77
- WHO_ROLE_RESOLVED: 2
- WHO_PERSON_RESOLVED: 0
- PUBLIC_CONTACT_PATH_AVAILABLE: 3
- NOT_RESEARCHED: 75
- pathClasses: {"NOT_RESEARCHED":75,"FUNCTIONAL":2}

### AC (n=32)
- WHO_ENTITY_RESOLVED: 32
- WHO_ROLE_RESOLVED: 5
- WHO_PERSON_RESOLVED: 1
- PUBLIC_CONTACT_PATH_AVAILABLE: 5
- NOT_RESEARCHED: 27
- pathClasses: {"NOT_RESEARCHED":27,"NAMED_PARTIAL":1,"ORG_PATH":3,"FUNCTIONAL":1}

### BETHESDA (n=54)
- WHO_ENTITY_RESOLVED: 54
- WHO_ROLE_RESOLVED: 38
- WHO_PERSON_RESOLVED: 29
- PUBLIC_CONTACT_PATH_AVAILABLE: 39
- NOT_RESEARCHED: 15
- pathClasses: {"NAMED_DIRECT":19,"NOT_RESEARCHED":15,"ORG_PATH":9,"NAMED_PARTIAL":10,"NO_CONTACT_AFTER_RESEARCH":1}

### RENAISSANCE (n=29)
- WHO_ENTITY_RESOLVED: 29
- WHO_ROLE_RESOLVED: 22
- WHO_PERSON_RESOLVED: 22
- PUBLIC_CONTACT_PATH_AVAILABLE: 23
- NOT_RESEARCHED: 6
- pathClasses: {"NAMED_DIRECT":13,"NOT_RESEARCHED":6,"NAMED_PARTIAL":9,"NO_CONTACT_AFTER_RESEARCH":1}

### HILTON (n=45)
- WHO_ENTITY_RESOLVED: 45
- WHO_ROLE_RESOLVED: 27
- WHO_PERSON_RESOLVED: 27
- PUBLIC_CONTACT_PATH_AVAILABLE: 30
- NOT_RESEARCHED: 15
- pathClasses: {"NOT_RESEARCHED":15,"NAMED_DIRECT":16,"NAMED_PARTIAL":11,"NO_CONTACT_AFTER_RESEARCH":3}

## Blocking assessment

Structural surface-eligible rows failing solely on `who_research_not_attempted`: **17** across subjects.

WHO requirement blocking otherwise-valid opportunities: **YES** — mostly as **research-not-attempted** (missing ceiling stamp), not as named-person hard requirement.

Controls (Bethesda/NYC) show far higher NAMED_* / ORG_PATH rates — WHO enrichment depth differs by market maturity, not by a different code path.
