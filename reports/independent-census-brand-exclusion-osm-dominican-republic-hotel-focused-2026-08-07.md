# Independent Census — Brand Exclusion Audit

**Status:** `independent_census_brand_exclusion_audit_ready`
**Version:** independent-census-brand-exclusion-v1
**Generated:** 2026-08-07T08:06:36.886Z
**Batch:** osm-dominican-republic-hotel-focused-2026-08-07
**Airtable writes:** no

## Summary

| Metric | Count |
| --- | ---: |
| OSM candidates | 920 |
| Active Brand Setup brands (dictionary) | 62 |
| Route → branded Autopilot (Active/Live) | 19 |
| Known chain hold (not Active/Live) | 114 |
| Steward possible branded | 27 |
| Weak identity hold | 0 |
| Independent unaffiliated pool | 760 |
| Independent missing city | 651 |
| Independent missing website | 677 |
| Independent likely already in legacy Census | 50 |
| Independent likely new vs legacy | 686 |
| Independent high quality (≥70) | 201 |
| L1 promote-ready proxy (name+country+website+q≥55, not likely_existing) | 75 |

## Route counts

| Route | Count |
| --- | ---: |
| `route_branded_active_setup` | 16 |
| `route_branded_soft_collection` | 1 |
| `route_branded_official_domain` | 2 |
| `route_known_chain_not_active` | 114 |
| `steward_possible_branded` | 27 |
| `independent_unaffiliated_candidate` | 760 |
| `hold_weak_identity` | 0 |

## Top matched brands (excluded from independent lane)

| Brand | Count |
| --- | ---: |
| RIU | 14 |
| Be Live | 13 |
| Barceló | 9 |
| Bahía Príncipe | 9 |
| Hodelpa | 8 |
| Iberostar | 8 |
| Dreams (Hyatt Inclusive Collection) | 5 |
| Wyndham | 5 |
| Meliá | 5 |
| Los Corales Village - Grupo Dos | 5 |
| Catalonia | 3 |
| Occidental | 3 |
| Excellence Resorts | 3 |
| Hilton Hotels & Resorts | 3 |
| Majestic Resorts | 3 |
| Crowne Plaza | 2 |
| Hard Rock Hotels | 2 |
| InterContinental | 2 |
| Breathless (Hyatt Inclusive Collection) | 2 |
| Amhsa Marina Hotels | 2 |
| Lopesan | 2 |
| Marriott | 2 |
| Holiday Inn | 2 |
| Westin | 2 |
| Karisma Hotels | 2 |

## Promote-ready sample (independent lane)

| Name | City | Website | Quality |
| --- | --- | --- | ---: |
| Paraiso Caño Hondo |  | https://www.paraisocanohondo.com/home.aspx | 80 |
| Hotel Europa | Sosua | http://www.hotelplazaeuropa.com | 100 |
| Hotel Atarazana |  | http://www.hotel-atarazana.com | 85 |
| Hotel Casablanca |  | http://www.hotelcasablanca.com.do/de-ch/ | 80 |
| Hotel Orchidee |  | http://www.hotelorchidee.ch | 70 |
| Hotel Palapa |  | https://www.palapabeachclub.com/ | 70 |
| Las Olas Luxury Villas |  | https://www.dominicana.life/ | 70 |
| Hotel Plaza Coral |  | http://hotel.plazacoral.net | 80 |
| Centro Ecoturistico Nalga de Maco |  | https://es-es.facebook.com/media/set/?set=a.341349045923308.79631.155184987873049&type=3 | 70 |
| Bungalows of Las Galeras | Las Galeras | http://bungalowsoflasgaleras.com | 100 |
| La Hacienda |  | http://www.lahaciendahostel.com/ | 80 |
| Hotel Docia |  | https://facebook.com/profile.php?id=208183975884379 | 70 |
| Sans Souci Appartments |  | http://sanssoucicabarete.com/ | 70 |
| Monte Placido |  | https://www.monteplacido.com/ | 70 |
| Jarabacua River Club & Resort |  | http://riverclubjarabacoa.com/ | 80 |
| Bella Vista | Santo domingo | www.hotelbellavista.net | 100 |
| Dan y Manty's Hostel and Guesthouse |  | lasterrenashostels.com | 85 |
| Dan and Manty's Guesthouse and Hostel |  | lasterrenashostels.com | 85 |
| El Pelicano Apart-Hotel | Las Galeras | https://www.elpelicanosamana.com/ | 100 |
| La Castilla |  | http://lacastillacolonial.com | 70 |
| Tubagua Plantation Eco Lodge |  | https://www.tubagua.com/ | 80 |
| Villa Ixora |  | https://www.cariway.com/ | 85 |
| Villa Flamboyan |  | https://www.cariway.com/ | 85 |
| Jasmine La Playita |  | https://www.cariway.com/ | 85 |
| Villa Diana |  | https://www.cariway.com/ | 80 |
| Villa Maxime |  | https://www.cariway.com/ | 85 |
| Villa Caribeña |  | https://www.cariway.com/ | 80 |
| Solazul |  | http://www.elsolazul.com | 80 |
| Sol Azul |  | http://www.elsolazul.com | 70 |
| La Casa Amarilla |  | http://www.la-casa-amarilla.com | 75 |

## Learning taxonomy (batch-learning)

- `learned_code_rule`: brand-exclusion gate via Active/Live dictionary + official domains
- `learned_validation_rule`: independent L1 proxy requires website + quality ≥ 55 + not legacy duplicate
- `Webhound_candidate`: residual hard cases = independent with no website + no city (after OSM+Wikidata)
- Do **not** send branded_route rows to Webhound; route to Autopilot coverage

## Next

1. Wikidata dry-run + evidence attach for promote-ready independents
2. Steward review `steward_possible_branded`
3. Gated promote-plan dry-run into Hotel Property Census (Affiliation Status = Independent)
4. Optional Webhound only on 10–25 hard residual cases
