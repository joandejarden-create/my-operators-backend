# Multilingual Parser Audit

| Sample | Detected lang | Buyer roles | Lodging class | Epistemic |
|--------|---------------|-------------|---------------|-----------|
| Secretaría Técnica · Responsable de Alojamiento · hotel oficial · bloq… | es | HOUSING_CONTROLLER|TECHNICAL_SECRETARIAT | DIRECT_LODGING_EVIDENCE | Fact |
| Comité Organizador e expositores — hoteles recomendados e aloxamento… | gl | ORGANIZING_COMMITTEE|EXHIBITOR_SERVICES|HOUSING_CONTROLLER | STRONG_HOTEL_MOTION | Fact |
| Xornadas de enerxía e naval — hoteis recomendados A Coruña 2027… | gl | EVENT_COORDINATOR | STRONG_HOTEL_MOTION | Fact |
| Convenio hotelero · agencia oficial de viajes · tarifa preferencial… | unknown | OFFICIAL_TRAVEL_AGENCY|TRAVEL_DESK | DIRECT_LODGING_EVIDENCE | Fact |
| The organizing committee lists housing bureau and official hotel… | en | ORGANIZING_COMMITTEE|HOUSING_CONTROLLER | DIRECT_LODGING_EVIDENCE | Fact |
| solo hotel sin más contexto… | en | — | PLAUSIBLE_HOTEL_MOTION | Fact |

## Coverage

- Dates / orgs / events: deferred to existing packet parsers; role + lodging language verified above.
- Bare "hotel" alone → PLAUSIBLE or UNCONFIRMED — **not** DIRECT.
- Relevant roles map to TECHNICAL_SECRETARIAT / HOUSING_CONTROLLER / etc. — **not** GENERAL_ORG_CONTACT.
