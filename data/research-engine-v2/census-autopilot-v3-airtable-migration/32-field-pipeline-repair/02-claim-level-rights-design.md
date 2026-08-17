# Claim-level rights selection

Version: `census-autopilot-v3.0.1-claim-store`

## Rule
A blocked lower-authority claim must **never** suppress a permitted higher-authority claim for the same field.

## Flow
RESEARCH CLAIM → FIELD-SPECIFIC SOURCE SELECTION → BEST ELIGIBLE CLAIM → GOLDEN STAGING → WRITE CLASS → AIRTABLE

## API
`resolveBestEligibleClaim(claims)` returns selected_claim, selected_source, selected_rights_status, rejected_claims_with_reason.

SerpApi-only → BLOCKED_RIGHTS (rejected). Official alongside SerpApi → official selected.
