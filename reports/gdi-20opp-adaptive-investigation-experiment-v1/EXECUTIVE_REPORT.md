# GDI 20-Opportunity Adaptive Investigation Experiment — Executive Report

**Experiment:** gdi_20opp_adaptive_investigation_experiment_v1  
**Version:** 2026-11-19.1  
**Executed:** 2026-11-19T07:31:31.054Z  
**Paid spend (actual):** $0 (cap $0)

## Recommendation

**ITERATE** — Offline arm improves local recovery/planning scores, but lift is not yet evidence of new verified web findings or eligibility flips — do not treat as live-agent validation

Next: Tighten challenger→eligibility coupling; run gated ≤$20 public-fetch subset; re-score with blind human review

## Headline metrics

| Metric | Current GDI (baseline) | Agentic GDI | Difference |
|--------|------------------------|-------------|------------|
| Avg rubric total | 26.90 | 31.45 | 4.55 |
| Cases improved | — | 20 | — |
| Cases worsened | — | 0 | — |
| Cases unchanged | — | 0 | — |
| Actual paid USD | 0 | 0 | 0 |

## Execution fidelity

Default run uses frozen snapshots + deterministic ORS/eligibility + offline adaptive planning. No paid providers. Historical publication statuses are not claimed as newly executed baselines.

Gated live protocol: **NOT_EXECUTED** (max incremental $20, founder auth required).

## Cohort

- **Case 1** `rx_d_096138499154` — Insurance Bureau of Canada @ Radisson Blu Toronto Downtown [corporate_meetings]
- **Case 2** `disp_recLuxvwwxID7U2B8_aledade` — Aledade @ Bethesda Marriott [corporate_meetings]
- **Case 3** `gdi_lmc_00562bf81b1e1957` — CMAA San Diego Chapter @ Wyndham San Diego Bayside [corporate_meetings]
- **Case 4** `rx_d_05bc95afb22f` — CCI France Suisse @ YOTEL Geneva Lake [corporate_meetings]
- **Case 5** `camp_efpa_2026_castillo` — EFPA @ Castillo Hotel Son Vida [association_event]
- **Case 6** `hist_e3bf667d9347` — PALCCO — Palacio de la Cultura y los Congresos @ Sheraton Guadalajara Expo [association_event]
- **Case 7** `gdi_opp_governor_s_hurricane_conference_2027_26` — Governor’s Hurricane Conference @ Waterstone Resort & Marina [association_event]
- **Case 8** `wy_cmi_0b26c2c38333` — Cléo @ Wyndham Alltra Punta Cana [association_event]
- **Case 9** `rx_d_5976330f194d` — International Chamber of Commerce / World Chambers Federation and Ontario Chamber of Commerce @ Radisson Blu Toronto Downtown [projects_workforce]
- **Case 10** `gdi_opp_2027_ddaa_annual_conference_8` — Development District Association of Appalachia (DDAA) @ Bethesda Marriott [projects_workforce]
- **Case 11** `rx_d_3aba886425f2` — Swiss Congress @ YOTEL Geneva Lake [projects_workforce]
- **Case 12** `rb_gdi_disc_dc428c1bd4c4e412` — International Society for Cell & Gene Therapy @ Radisson Blu Toronto Downtown [medical_pharma]
- **Case 13** `disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_` — Cohera (formerly CSI DMC and 360DG) @ Bethesda Marriott [medical_pharma]
- **Case 14** `gdi_opp_prospect_mv8p5ddh_7edb8e7c` — American Association of Neurological Surgeons @ Wyndham San Diego Bayside [medical_pharma]
- **Case 15** `gdi_opp_wrcamp_luiss_executive_2027_luiss_business_school_program_host` — LUISS Business School @ W Rome [sports_university]
- **Case 16** `gdi_opp_2026_11u_pg_west_coast_showdown_5_2no8ma1o` — Perfect Game @ Wyndham San Diego Bayside [sports_university]
- **Case 17** `gdi_opp_ajas_hq_rockville_watch` — Association of Jewish Aging Services (AJAS) @ Bethesda Marriott [rejected_unsupported]
- **Case 18** `gdi_opp_cmss_spring_2027` — Council of Medical Specialty Societies (CMSS) @ Bethesda Marriott [rejected_unsupported]
- **Case 19** `gdi_opp_wrcamp_altaroma_2027_altaroma_organizer` — Altaroma @ W Rome [cross_hotel_booked]
- **Case 20** `camp_ivirma_2027_castillo` — IVIRMA / Grupo Pacífico housing @ Castillo Hotel Son Vida [cross_hotel_booked]

## Detailed examples

### strongestImprovement
- Opportunity: `gdi_lmc_00562bf81b1e1957` (CMAA San Diego Chapter)
- Hotel: Wyndham San Diego Bayside
- Delta: 6

### strongestRejectionOrCorrection
- Opportunity: `gdi_opp_ajas_hq_rockville_watch` (Association of Jewish Aging Services (AJAS))
- Hotel: Bethesda Marriott
- Delta: 3

### contactImprovement
- Opportunity: `gdi_lmc_00562bf81b1e1957` (CMAA San Diego Chapter)
- Hotel: Wyndham San Diego Bayside
- Delta: 6

### crossHotelReuse
- Opportunity: `gdi_opp_wrcamp_altaroma_2027_altaroma_organizer` (Altaroma)
- Hotel: W Rome
- Delta: 6

### noValueOrWorse
- Opportunity: `gdi_opp_ajas_hq_rockville_watch` (Association of Jewish Aging Services (AJAS))
- Hotel: Bethesda Marriott
- Delta: 3


## Moat

- Orgs touched: 20
- Multi-hotel orgs in cohort: 0
- Adaptive strategies proposed: 20

## Production note

Do **not** treat archived/frozen experiment outputs as Airtable production persistence or customer publication.
