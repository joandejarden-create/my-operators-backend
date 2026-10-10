# Buyer / Contact Path Taxonomy

| Class | Definition | READY eligible? |
|---|---|---|
| SOURCE_PAGE | Evidence / event landing page only | **NO** |
| GENERAL_ORG_CONTACT | Org homepage / generic root | **NO** (alone) |
| RELEVANT_FUNCTION_CONTACT | Public events/housing/travel/sourcing/when-where path | **YES** |
| NAMED_BUYER_ROLE_PATH | Named org + relevant buyer function/role | **YES** (not homepage+Housing without lodging evidence) |
| NAMED_BUYER_PERSON | Named public individual (`isLikelyPersonName`) | **YES** |

## Housing desk rule

`Housing` / `Hotel Reservation` role + SOURCE_PAGE/GENERAL_ORG homepage URL **without** official housing URL or lodging-program evidence → **not READY** (`housing_role_without_housing_path`).

## Bethesda preserve

Named org + functional desk (`… staff`, `… services`) + meetings/housing role with empty URL remains `NAMED_BUYER_ROLE_PATH` (no homepage falsely claiming housing desk).

Module: `lib/group-demand-intelligence/buyer-contact-path-taxonomy-v1.js`  
Gate: `meetsReadyContactRequirement` → `isGdiCustomerOpportunityReady`.
