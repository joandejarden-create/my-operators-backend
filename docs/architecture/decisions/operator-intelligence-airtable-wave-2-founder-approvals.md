# ADR — Operator Intelligence Airtable Wave 2 Founder Approvals

**Status:** Accepted  
**Date:** 2026-08-04  
**Related:** `docs/data/operator-intelligence-airtable-architecture.md`, calibration founder review

---

## Approved

### Existing-field population (Group A)

Apply reviewed values for the six calibration operators where:

- Destination field already exists  
- Value supported by approved source policy  
- No unresolved high-severity conflict  
- Value uses approved taxonomy  
- Dry-run identifies operation as safe  
- Existing value is blank, invalid, stale, or explicitly superseded  

### Cenote Azul normalization

Apply geography correction: remove unsupported country coverage; preserve source trail and change reason. Do not represent “no evidence found” as confirmed absence.

### Minimum evidence architecture

Create/ensure only structures required for calibration persistence and scaling research:

- Structured claims  
- Evidence sources (prefer Partner Intelligence Source Library)  
- Operator comparable assignments (extend Case Studies)  
- Brand–operator relationships (existing Brand Relationships table)  
- Verification / publication status, geographic and brand scope, refresh dates, exception review  

Do **not** create deferred project-specific commercial/performance structures.

### Publication methodology

Deterministic classes remain approved: Auto-Publish · Publish With Evidence Label · Internal Only · Human Review Required · Rejected · Stale · Conflicted · Insufficient Evidence. Routine objective facts meeting policy do not require founder field approval.

### Wave 2 research

Approve Wave 2 focused on coverage gaps. Initial candidates: Highgate, Atlantica, Driftwood, Santa Fe. Include Remington only if selection analysis shows higher immediate calibration value.

## Not approved

My Deals wiring · owner feature enablement · intake / Brand Match v2 / OAS changes · OAS deprecation · shortlist persistence · ODR-as-shortlist · pathway matrix · owner weighting · unsupported performance · project-specific fees/availability from general research · auto-publish sensitive negatives · outreach automation.
