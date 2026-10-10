# Demand Controller Model

A **Demand Controller** is the entity/function that materially influences lodging selection, booking, allocation, or hotel-list inclusion.

## Types
- PCO
- DMC
- HOUSING_BUREAU
- ASSOCIATION_SECRETARIAT
- HOST_INSTITUTION
- CORPORATE_TRAVEL
- PROCUREMENT
- EVENT_AGENCY
- TRAVEL_MANAGEMENT_COMPANY
- CONVENTION_BUREAU
- VENUE_CONGRESS_OFFICE
- SPORTS_TRAVEL
- PRODUCTION_COORDINATOR
- REGISTRATION_VENDOR
- UNKNOWN

## Authority
- CONFIRMED_LODGING_CONTROLLER
- STRONG_SELECTION_INFLUENCE
- PLAUSIBLE_CONTROLLER
- CONTACT_ONLY
- UNCONFIRMED

## Rules
- Do **not** infer CONFIRMED/STRONG authority from generic organizer status alone.
- Require lodging-role evidence (housing page, PCO accommodation, reservation portal, etc.).
- Not customer-facing as a technical object — use `customerSafeControllerCopy`.

## Persistence
`data/group-demand-intelligence/international-discovery-v2/demand-controllers.json`
