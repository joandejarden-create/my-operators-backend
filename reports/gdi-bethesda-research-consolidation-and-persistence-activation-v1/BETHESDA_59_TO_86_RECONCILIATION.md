# Bethesda 59 → 86 reconciliation

| Set | Count |
|-----|------:|
| Canonical HEAD opportunities | 59 |
| Preserved snapshot | 86 |
| Shared (must keep) | 59 |
| Only in preserved (CREATE candidates) | 27 |
| Only in canonical | 0 |
| Shared title/org drift | 24 |
| Prior #372 manifest overlap | 27/27 |

## Source-of-truth decision

**CANONICAL_HEAD_59_PLUS_PRESERVED_86_SUPERSET_FOR_AUTHORIZED_CREATES**

- Canonical on main: `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json` (59 records) — **do not overwrite/delete**.
- Preserved additive superset: `wt-gdi-isolation-post-365/data/group-demand-intelligence/_wip-preserve-20261118/recLuxvwwxID7U2B8-opportunities.86.json` (86).
- Airtable expected baseline for Bethesda: **59**.
- Authorize only the **27** composite keys absent from canonical/Airtable.

## Create IDs

- `disp_recLuxvwwxID7U2B8_aledade`
- `disp_recLuxvwwxID7U2B8_city_of_rockville`
- `disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_`
- `disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce`
- `disp_recLuxvwwxID7U2B8_iwlca_elite_tournaments_iwlca_champions_cup`
- `disp_recLuxvwwxID7U2B8_linder_global_events`
- `disp_recLuxvwwxID7U2B8_montgomery_county_shrm`
- `disp_recLuxvwwxID7U2B8_nature_forward`
- `disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_`
- `disp_recLuxvwwxID7U2B8_pmi_montgomery_county_maryland_chapter`
- `disp_recLuxvwwxID7U2B8_strathmore`
- `gdi_beth_v16_evt_0ab91e208405`
- `gdi_beth_v16_evt_90a6ddcb10ca`
- `gdi_e2d_bc0a94d7c6007f36`
- `gdi_pep_live_af0711e711ba`
- `gdi_pep_live_c165c897d233`
- `gdi_plp_live_4958569d826e`
- `gdi_plp_live_fa2063dc1195`
- `gdi_s4912_364713d41c67`
- `gdi_s4912_512deebd0963`
- `gdi_s4912_59382d1e71a2`
- `gdi_s4912_66a1f89e5afb`
- `gdi_s4912_6c8a62552bfa`
- `gdi_s4912_6ffc303bf55a`
- `gdi_s4912_732496c2f8aa`
- `gdi_s4912_b0ea89e0716d`
- `gdi_s4912_d55ee8a8e9ca`
