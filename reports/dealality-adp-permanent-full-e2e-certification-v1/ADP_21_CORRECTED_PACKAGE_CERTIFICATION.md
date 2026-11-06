# ADP 21 Corrected Package Certification

Locally prepared corrected immutable packages for Group B. **NOT published. NOT production-activated.**

Location: `reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-packages/`

| propertyId | measurement | package | parity | Consideration unchanged |
|---|:-:|:-:|:-:|:-:|
| adp_casas_del_xvi | PASS | PASS | PASS | YES |
| adp_castillo_hotel_son_vida | PASS | PASS | PASS | YES |
| adp_faranda_collection_bogota | PASS | PASS | PASS | YES |
| adp_fiesta_americana_condesa_cancun_ai | PASS | PASS | PASS | YES |
| adp_grand_fiesta_americana_chapultepec | PASS | PASS | PASS | YES |
| adp_hilton_times_square | PASS | PASS | PASS | YES |
| adp_hotel_caribe_faranda_grand | PASS | PASS | PASS | YES |
| adp_jw_marriott_monterrey_valle | PASS | PASS | PASS | YES |
| adp_jw_marriott_santo_domingo | PASS | PASS | PASS | YES |
| adp_radisson_blu_toronto_downtown | PASS | PASS | PASS | YES |
| adp_radisson_santo_domingo | PASS | PASS | PASS | YES |
| adp_sheraton_guadalajara_expo | PASS | PASS | PASS | YES |
| adp_sheraton_mallorca_arabella_golf | PASS | PASS | PASS | YES |
| adp_spice_island_beach_resort | PASS | PASS | PASS | YES |
| adp_st_regis_cap_cana | PASS | PASS | PASS | YES |
| adp_st_regis_mexico_city | PASS | PASS | PASS | YES |
| adp_umusic_hotel_madrid | PASS | PASS | PASS | YES |
| adp_voco_cancun_zona_hotelera | PASS | PASS | PASS | YES |
| adp_westin_monterrey_valle | PASS | PASS | PASS | YES |
| adp_wyndham_alltra_punta_cana | PASS | PASS | PASS | YES |
| adp_yotel_geneva_lake | PASS | PASS | PASS | YES |

## Rules applied

1. Re-adjudicate mentions from rawResponse via detectPropertyMention
2. Rebuild Provider Presence + Scenario Presence from frozen eligibleScenarioIds
3. Consideration numerators/denominators/rate unchanged vs prior certified package
4. Prior publicationManifestSha256 / pdfArtifactSha256 retained as historical references
5. publicationAuthorized=false, published=false on every prepared package