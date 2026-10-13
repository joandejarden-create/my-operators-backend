# CHANGELOG — International Discovery V2

## Added
- `lib/group-demand-intelligence/international-discovery-v2/` — spines, Demand Controller, market profiles, path/blocker engines, equivalent evidence, intermediary graph, hotel-supplied feedback, multilingual ontology, source families, Ready-gate intl audit.
- Pursuit `recordPursuitResponse` → IDV2 hotel-supplied feedback loop (no auto-promote).
- Report pack under `reports/gdi/international-discovery-v2/`.

## Changed (equivalent evidence — not threshold lowering)
- **functional_path_multilingual**: Accept ES/DE/GL equivalents: alojamiento, hoteles, reserva, unterkunft, hotelkontingent, zimmerkontingent, partnerhotel, kontakt, anmeldung, inscripción (NONE — same RELEVANT_FUNCTION_CONTACT class; more proof forms for same fact)
- **relevant_role_multilingual**: Accept secretaría técnica, alojamiento, unterkunft, kongress, tagung, einkauf, contratación, pco, dmc, agencia de eventos, Veranstaltungsagentur (NONE)
- **lodging_blob_multilingual**: Accept hotel oficial, hoteles recomendados, offizielles Hotel, Partnerhotel, Hotelkontingent, bloque de habitaciones, alojamiento oficial (NONE — still requires positive lodging evidence; negated phrasing still rejected)
- **functional_desk_multilingual**: Accept secretaría, alojamiento, Kongressbüro, Teilnehmermanagement as desk-style names (NONE)
- **named_person_not_required_when_functional_pco**: meetsReadyContactRequirement already accepts RELEVANT_FUNCTION / NAMED_BUYER_ROLE_PATH; multilingual expansion makes functional PCO/secretariat paths recognizable internationally (NONE)
- **public_hotel_block_vs_pco_process**: Equivalent Evidence Policy allows PCO accommodation instructions / hotel-supplied rate request / procurement notice as alternate forms for LODGING_SELECTION_ACTIVE — maturity gates unchanged; lodgingEvidenceClass taxonomy unchanged (NONE)
- **participant_list_not_sole_path**: Demand Controller First + Account First spines can resolve buyer/controller and accounts without lists; Ready still requires full pillar set (NONE)

## Explicitly not changed
- COMPLETE_STRONG / COMPLETE_PLAUSIBLE / PARTIAL_PACKET / SIGNAL_ONLY definitions
- DIRECT_LODGING_EVIDENCE / STRONG_HOTEL_MOTION / PLAUSIBLE_HOTEL_MOTION / UNCONFIRMED / NONE taxonomy
- Valid Future Watch gate
- No speculative lodging
- No auto-promote from outreach alone
- Historical evidence cannot prove current placement
