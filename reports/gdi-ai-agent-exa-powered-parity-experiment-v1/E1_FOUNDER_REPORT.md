# GDI AI Agent — E1 Live Exa-Parity Founder Report

**Status:** E1_COMPLETE
**Commercial verdict:** REJECT
**Actual paid USD:** $0.8800 / $20.00 ceiling
**Authorization:** gdi-ai-agent-exa-parity-e1-founder-20usd-v1
**Harness:** gdi-ai-agent-exa-parity-e1-harness-v1

## Verdict

- Independent challenger verdict: **REJECT**
- Primary reason: **no incremental commercial intelligence** vs existing Exa-powered GDI (0/8 evidence-grounded materialLift cases; named-contact and new-evidence deltas tied on every case).
- Automated gate also REJECT (FALSE_SALES_READY_PRESSURE on ISCT) — that pattern appears on **both** arms from the frozen baseline; Arm B stopped with challenger guard, so it is not unique adaptive false-promotion pressure.
- Adaptive planning did run (distinct sequences + reformulations) and correctly demoted speculative/competitor/forward-looking slots, but that is not material commercial lift over Arm A.
- ADOPT would mean further selective validation only — not production deployment. Not warranted here.

## Guardrails

- Airtable writes: false
- Customer publication: false
- Production persistence: false
- Extra paid providers: denied (serpapi, pdl, surfe, firecrawl, parallel, jev, exa_live_http)
- Cohort: frozen eight cases only

## Eight-case side-by-side

### disp_recLuxvwwxID7U2B8_aledade — incomplete_demand — Bethesda Marriott

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 3 | 0 | RESEARCH_CONTINUATION | $0.1050 | 3 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 3 | 0 | RESEARCH_CONTINUATION | $0.1050 | 3 | STOP_RESEARCH_CONTINUATION_CHALLENGER_GUARD |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 3

### disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_ — incomplete_demand — Bethesda Marriott

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 3 | 1 | RESEARCH_CONTINUATION | $0.1050 | 3 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 3 | 1 | RESEARCH_CONTINUATION | $0.1050 | 3 | STOP_ADAPTIVE_COMPLETE |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 2

### rb_gdi_disc_dc428c1bd4c4e412 — missing_verified_contact — Radisson Blu Toronto Downtown

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 3 | 0 | SALES_READY | $0.1050 | 3 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 3 | 0 | SALES_READY | $0.1050 | 3 | STOP_RESEARCH_CONTINUATION_CHALLENGER_GUARD |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 2

### gdi_opp_prospect_mv8p5ddh_7edb8e7c — missing_verified_contact — Wyndham San Diego Bayside

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 3 | 1 | RESEARCH_CONTINUATION | $0.1050 | 3 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 3 | 1 | RESEARCH_CONTINUATION | $0.1050 | 3 | STOP_ADAPTIVE_COMPLETE |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 2

### gdi_opp_governor_s_hurricane_conference_2027_26 — forward_looking — Waterstone Resort & Marina

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | STOP_CORRECT_DEMOTION |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 0

### gdi_opp_ajas_hq_rockville_watch — rejected_speculative — Bethesda Marriott

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | STOP_CORRECT_DEMOTION |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 0

### gdi_opp_wrcamp_altaroma_2027_altaroma_organizer — competitor_cross_hotel — W Rome

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | STOP_CORRECT_DEMOTION |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 0

### camp_ivirma_2027_castillo — competitor_cross_hotel — Castillo Hotel Son Vida

| Arm | Evidence | Named contacts | Eligibility | Paid USD | Fetches | Stop |
|-----|----------|----------------|-------------|----------|---------|------|
| A existing Exa GDI | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | EXISTING_POLICY_SEQUENCE_COMPLETE |
| B Exa + adaptive | 2 | 0 | RESEARCH_CONTINUATION | $0.0050 | 2 | STOP_CORRECT_DEMOTION |

- Material lift: no (none)
- Arm A contacts: —
- Arm B contacts: —
- Adaptive reformulations (B): 0

## Spend by case

- disp_recLuxvwwxID7U2B8_aledade: A=$0.1050 B=$0.1050 total=$0.2100
- disp_recLuxvwwxID7U2B8_cohera_formerly_csi_dmc_and_360dg_: A=$0.1050 B=$0.1050 total=$0.2100
- rb_gdi_disc_dc428c1bd4c4e412: A=$0.1050 B=$0.1050 total=$0.2100
- gdi_opp_prospect_mv8p5ddh_7edb8e7c: A=$0.1050 B=$0.1050 total=$0.2100
- gdi_opp_governor_s_hurricane_conference_2027_26: A=$0.0050 B=$0.0050 total=$0.0100
- gdi_opp_ajas_hq_rockville_watch: A=$0.0050 B=$0.0050 total=$0.0100
- gdi_opp_wrcamp_altaroma_2027_altaroma_organizer: A=$0.0050 B=$0.0050 total=$0.0100
- camp_ivirma_2027_castillo: A=$0.0050 B=$0.0050 total=$0.0100

## Independent challenger note

Independent evidence challenger + evidence-grounded materialLift; drawerReady-only excluded; double-count avoided via paired baseline deltas.

