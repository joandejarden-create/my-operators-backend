# First-Party Validation Model (V2.2)

## Purpose
Unresolved Rooms (and selected hard fields) are **not** Autopilot failures. They become an operational validation layer during brand/operator onboarding.

## Package concept
> Dealality independently maintains the following information for your portfolio. Please confirm or correct these fields.

Fields: Hotel Name, Brand, Property ID, Rooms / Keys, Operating Status, Opening Date, Operator / Management Company, Address

**Never include** Cvent or legacy provenance in validation packages.

## Ingestion contract
Every confirmed value carries:

| Field | Rule |
|-------|------|
| `source_type` | `FIRST_PARTY_VALIDATION` |
| `organization` | Brand or management company |
| `respondent` | Optional contact |
| `confirmation_date` | ISO date |
| `field` | Canonical Census field |
| `confirmed_value` | Value as confirmed |
| `prior_value` | Prior Dealality value or null |
| `change_type` | confirm \| correct \| supply_new |
| `evidence_reference` | Ticket/email/upload id |
| `confidence` | **HIGH** |

First-party confirmation is among the strongest Census source classes — stronger than SerpApi staging and secondary web.

## Write class
Maps to `FIRST_PARTY_VALIDATION` / Class D preferred fields (Rooms, Operator, Opening Date).

## UI
External onboarding UI is **out of scope** for V2.2 unless trivial. This artifact is the data model + target generation only.
