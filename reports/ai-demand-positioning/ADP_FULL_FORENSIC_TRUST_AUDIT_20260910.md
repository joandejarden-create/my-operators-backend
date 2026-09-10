# ADP Full Forensic Trust Audit — 2026-09-10

## Overall verdict

**TRUSTED**

External distribution: **LIFTED** (`CLIENT_READY`)

## Santo Domingo screenshot (obs_cb9ec532808d)

- Provider: Gemini · Scenario: `std_sdq_wel_02` · Territory: Wellness
- Subject Path-A present: **false**
- Raw hash: `faf3719f88ba6030…`
- Stored InterContinental present post-reprocess: **true**
- Live extractor includes InterContinental: **true**
- InterContinental in overall competitive table: **true**

### Why title said Positive Evidence · Business while record said Wellness / Displaced

Modal title Positive Evidence · Business is UI sticky-title / wrong opener state: displacement drawer content (Status=Displaced, Displacing Competitor, territory from observation) was shown while #adpEvidenceTitle retained a prior Positive Evidence · Business open. Territory Wellness comes from scenario std_sdq_wel_02 → intent wellness. Evidence type Positive vs Displaced is renderer label parity failure, not raw observation truth.

### Why El Embajador was displacer while InterContinental was top recommendation

HISTORICAL ROOT CAUSE: InterContinental omitted from competitorsMentioned (no Hotel token; registry aliases not scanned) while El Embajador extracted → Embajador received displacement credit; Inter displacement count 0; omitted from competitive table. Governed law unchanged: every resolved competitor in a subject-absent scenario gets scenario credit (not #1-only). POST-REPROCESS 2026-09-10: InterContinental is extracted, appears in overall table, and is Top Observed AI Alternative; El Embajador remains a valid co-observed displacer.

### Malformed chips

HISTORICAL: competitorsMentioned stored prose spans containing hotel/resort as common nouns; UI rendered chips verbatim. POST-REPROCESS: prose-fragment rejection + customer chip filter; screenshot obs junk cleared.

## Displacement law (current, unchanged)

> A displacement event is a monitored demand scenario where the subject hotel is not mentioned in any comparable provider response for that scenario, and the competitor hotel appears in at least one comparable provider response for that scenario.

Multiple competitors in one subject-absent scenario each receive scenario credit when they resolve to a canonical entityId. There is **no** “only #1 gets displacement” rule in production today.

## Property scorecard

| Property | Overall | Key defects |
|----------|---------|-------------|
| adp_bethesda_marriott | PASS | — |
| adp_cambridge_beaches_bermuda | PASS | — |
| adp_casas_del_xvi | PASS | — |
| adp_faranda_collection_bogota | PASS | — |
| adp_hotel_caribe_faranda_grand | PASS | — |
| adp_hotel_phillips_kansas_city | PASS | — |
| adp_jw_marriott_monterrey_valle | PASS | — |
| adp_jw_marriott_santo_domingo | PASS | — |
| adp_now_now_noho | PASS | — |
| adp_radisson_santo_domingo | PASS | — |
| adp_renaissance_times_square | PASS | — |
| adp_st_regis_cap_cana | PASS | — |
| adp_st_regis_mexico_city | PASS | — |
| adp_waterstone_boca_raton | PASS | — |
| adp_westin_monterrey_valle | PASS | — |

## Methodology changed

**NO**

## Safe to resume external distribution

**YES** — parallel paths CLOSED; HOLD lifts only at TRUSTED.
