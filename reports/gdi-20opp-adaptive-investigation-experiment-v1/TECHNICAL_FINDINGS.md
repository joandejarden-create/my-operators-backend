# Technical findings — gdi_20opp_adaptive_investigation_experiment_v1

## Arms

- **Baseline:** `evaluateOpportunity` + `evaluateSharedCommercialEligibility` + evidence challenger on frozen snapshots.
- **Agentic:** commercial evidence recovery + `buildAdaptiveResearchPreflight` + `selectAdaptiveNextAction` + challenger + eligibility (offline by default).

## Hard denies

Paid providers denied: exa, firecrawl, serpapi, surfe, pdl, parallel, jev

## Per-case comparison (compact)

| Case | Opportunity ID | Δ score | Baseline status | Agentic status | Worse? |
|------|----------------|---------|-----------------|----------------|--------|
| 1 | `rx_d_096138499154` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 2 | `disp_recLuxvwwxID7U2B8_aledade` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 3 | `gdi_lmc_00562bf81b1e1957` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 4 | `rx_d_05bc95afb22f` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 5 | `camp_efpa_2026_castillo` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 6 | `hist_e3bf667d9347` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 7 | `gdi_opp_governor_s_hurricane_conference_2027_26` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 8 | `wy_cmi_0b26c2c38333` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 9 | `rx_d_5976330f194d` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 10 | `gdi_opp_2027_ddaa_annual_conference_8` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 11 | `rx_d_3aba886425f2` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 12 | `rb_gdi_disc_dc428c1bd4c4e412` | 4 | SALES_READY | RESEARCH_CONTINUATION | false |
| 13 | `disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 14 | `gdi_opp_prospect_mv8p5ddh_7edb8e7c` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 15 | `gdi_opp_wrcamp_luiss_executive_2027_luiss_business_school_program_host` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 16 | `gdi_opp_2026_11u_pg_west_coast_showdown_5_2no8ma1o` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 17 | `gdi_opp_ajas_hq_rockville_watch` | 3 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 18 | `gdi_opp_cmss_spring_2027` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 19 | `gdi_opp_wrcamp_altaroma_2027_altaroma_organizer` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |
| 20 | `camp_ivirma_2027_castillo` | 6 | RESEARCH_CONTINUATION | RESEARCH_CONTINUATION | false |

## Substitutions / shortfalls

```json
{
  "substitutions": [
    {
      "hotelId": "recgMYovrrZDJMqzX",
      "hotelName": "Waterstone Resort & Marina",
      "reason": "included as substitute for preferred hotel without local opp store",
      "substituteFor": "Mayflower Autograph (rec0eTEnCqlsGJJt9 has profile only; no opportunities.json)"
    },
    {
      "hotelId": "rec82D9zpB8fede1I",
      "hotelName": "Castillo Hotel Son Vida",
      "reason": "included as substitute for preferred hotel without local opp store",
      "substituteFor": "Falls Church Marriott (no local opportunities.json in ws-gdi)"
    },
    {
      "hotelId": "recCAGqkERSXgiku4",
      "hotelName": "Sheraton Guadalajara Expo",
      "reason": "included as substitute for preferred hotel without local opp store",
      "substituteFor": "Nobu London (no local opportunities.json in ws-gdi)"
    },
    {
      "hotelId": "recXh4NgF8sDbi9cQ",
      "hotelName": "Wyndham Alltra Punta Cana",
      "reason": "included as substitute for preferred hotel without local opp store",
      "substituteFor": "additional coastal / leisure inventory diversity"
    }
  ],
  "shortfalls": []
}
```

## Decision object

```json
{
  "recommendation": "ITERATE",
  "rationale": "Offline arm improves local recovery/planning scores, but lift is not yet evidence of new verified web findings or eligibility flips — do not treat as live-agent validation",
  "avgDelta": 4.550000000000001,
  "eligibilityFlipCount": 1,
  "contactLiftCount": 0,
  "nextPhase": "Tighten challenger→eligibility coupling; run gated ≤$20 public-fetch subset; re-score with blind human review"
}
```
