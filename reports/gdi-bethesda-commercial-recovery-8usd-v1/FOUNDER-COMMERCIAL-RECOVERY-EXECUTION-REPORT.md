# Bethesda Marriott — Commercial Recovery & Paid Research Execution

**Authorization:** `FOUNDER_APPROVE_BETHESDA_COMMERCIAL_RECOVERY_8USD_20261112`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Hard ceiling:** $8.00 additional paid research  
**Actual spend:** **$2.40** (Exa Agent · 8 calls) · **Remaining:** $5.60  
**Auto-publish:** **No** · Customer feed unchanged at **19**

---

## Executive result

| Metric | Result |
|--------|--------|
| Candidates researched | 16 |
| New paid Exa calls | 8 |
| Actual spend | $2.40 |
| Commercially qualified | **3** |
| Sales Ready (new) | **0** |
| Account Development (new) | **3** |
| Publication-review-ready (staged) | **3** |
| Correctly rejected / continued | 13 |
| Cost per qualified opportunity | **$0.80** |
| Fresh discovery required | **No** |
| Local/published parity | **19 / 19 · store SHA unchanged** |

**Three strong Account Development opportunities are staged for founder publication review** (not customer-visible):

1. **Montgomery County SHRM** — local catering / chapter meetings  
2. **PMI Montgomery County Maryland Chapter** — monthly dinner / day meetings  
3. **Parenteral Drug Association (PDA)** — TRI Bethesda training lodging  

No Sales Ready promotion was forced: none has a confirmed hotel booking / open RFP. That is correct under the existing standard.

---

## 1. All 16 recovery candidates — final status

| ID | Organization | Final status | Paid? | Notes |
|----|--------------|--------------|-------|-------|
| `gdi_opp_arlington_spring_tournament_2027` | Arlington Soccer Association | RESEARCH_CONTINUATION | Yes $0.50 | Contested-metro / conditional stretch — lodging path open but not Bethesda-defensible for AD |
| `gdi_opp_nado_ddaa_washcon_2028` | NADO/DDAA | RESEARCH_CONTINUATION | Yes $0.50 | 2028 host already Crystal Gateway — overflow watch only |
| `gdi_opp_amwa_2027_annual` | AMWA | RESEARCH_CONTINUATION | Yes $0.50 | Host selected Arlington/Crystal City — weak Bethesda fit |
| `gdi_opp_nado_ddaa_washcon_2027_overflow` | NADO/DDAA | RESEARCH_CONTINUATION | Yes $0.10 | No official overflow housing path |
| `disp_…_montgomery_county_shrm` | Montgomery County SHRM | **PUBLICATION_REVIEW_READY_AD** | Yes $0.10 | Staged |
| `disp_…_pmi_montgomery_county_maryland_chapter` | PMI Montgomery County | **PUBLICATION_REVIEW_READY_AD** | Yes $0.10 | Staged |
| `disp_…_iwlca_…_champions_cup` | IWLCA Champions Cup | RESEARCH_CONTINUATION | Yes $0.50 | SoccerPlex demand real; primary revenue hypothesis / fit incomplete |
| `disp_…_cohera_…` | Cohera | RESEARCH_CONTINUATION | Yes $0.10 | Partnership potential only |
| `disp_…_parenteral_drug_association_pda_` | PDA | **PUBLICATION_REVIEW_READY_AD** | Prior $0.10 + rematerialize | Staged (no re-spend on exhausted lodging-buyer path) |
| `gdi_opp_series_high_risk_…_2027` | NIH Common Fund | RESEARCH_CONTINUATION | No | Speculative / monitor |
| `gdi_opp_world_biomaterials_2028_overflow` | WBC | RESEARCH_CONTINUATION | No | Overflow without open path |
| `gdi_opp_2027_ddaa_annual_conference_8` | DDAA | RESEARCH_CONTINUATION | No | Monitor |
| `gdi_opp_umd_alumni_weekend_watch` | UMD Alumni | RESEARCH_CONTINUATION | No | Watch |
| `gdi_opp_nih_sbpo_vos_pattern` | NIH SBPO | RESEARCH_CONTINUATION | No | Monitor |
| `gdi_opp_series_ctn_annual_conference_2027` | NIDA CTN | RESEARCH_CONTINUATION | No | Monitor |
| `gdi_opp_acvnu_renal_week_20270412` | ACVNU | RESEARCH_CONTINUATION | No | Overflow monitor |

---

## 2–3. Qualified vs rejected

**Qualified (3 Account Development · publication-review-ready):**

- Montgomery County SHRM — recurring chapter F&B / holiday / leadership programming in Bethesda–Rockville corridor; Neha Agarwal (Programs Hospitality Director).  
- PMI Montgomery County — recurring Rockville monthly dinner meetings; Matt Lowy.  
- PDA TRI Bethesda — same-city training lodging; Laura Keeley (Head of Education – TRI).

**Rejected / continued (13):** contested-metro stretch (Arlington Spring, AMWA, NADO overflow/2028), partnership-only (Cohera), or insufficient hotel-specific thesis / open housing path (remaining MONITOR AD-eligible rows). Not false negatives of the customer filter.

---

## 4–6. Paid research & new evidence

| Opportunity | Provider | Effort | Billed |
|-------------|----------|--------|--------|
| Arlington Spring 2027 | Exa Agent | high | $0.50 |
| NADO/DDAA 2028 | Exa Agent | high | $0.50 |
| AMWA 2027 | Exa Agent | high | $0.50 |
| NADO 2027 overflow | Exa Agent | medium | $0.10 |
| Montgomery County SHRM | Exa Agent | medium | $0.10 |
| PMI Montgomery County | Exa Agent | medium | $0.10 |
| IWLCA Champions Cup | Exa Agent | high | $0.50 |
| Cohera | Exa Agent | medium | $0.10 |
| **Total** | | | **$2.40** |

**Material new evidence (examples):**

- SHRM: documented 2022–2026 holiday + breakfast/dinner/leadership pattern; Manor Country Club incumbent for Dec 2026; 2027 calendar still the sell window.  
- PMI: That’s Amore Rockville monthly dinners + FedTALKS timing; hotel as alternative catering venue.  
- Arlington/NADO/AMWA: confirmed host/overflow honesty — correctly kept off AD staging for Bethesda.  
- PDA: prior Exa named Laura Keeley; rematerialization completed outreach + Bethesda TRI location anchors without re-spend.

---

## 7. Customer-facing bodies (founder review only)

Full bodies with ten-section drawers are in:

`reports/gdi-bethesda-commercial-recovery-8usd-v1/03-founder-publication-review-staging.json`

Research-stage bag (not canonical store):

`data/group-demand-intelligence/research-stage/recLuxvwwxID7U2B8/commercial-recovery-8usd-v1.json`

**Do not auto-publish.** Founder approval remains mandatory.

---

## 8. Research-to-publication workflow fixes

Substantiated gaps fixed in the existing Full E2E path:

1. **Demand-mechanism ignored `whatIsHappening`** — recovery Exa activity never counted as independent evidence → false SPECULATIVE. Fixed.  
2. **Negated “not … room blocks” falsely confirmed demand** — fixed.  
3. **Stale `RESEARCH_CONTINUATION` maturity permanently blocked AD** after evidence elevated — clears when CONFIRMED/RELIABLE_INFERRED.  
4. **Catering / chapter / training trigger patterns** too narrow for real local chapter dinners and TRI training courses — expanded.  
5. **CHD overwrote concrete recovery outreach with “monitor / research continuation”** — recovery finalize restores concrete outreach.  
6. **Ten-section + publicationReviewReady evaluation** now runs in the recovery finalization path; stages founder packages without `customerVisible`.

---

## 9–11. Counts

| | |
|--|--|
| Newly qualified Account Development | **3** |
| Newly qualified Sales Ready | **0** |
| Staged for publication review | **3** |

---

## 12. Fresh discovery

**Not required.** Remaining $5.60 reserved; expected value of broad rediscovery is lower than completing these three founder reviews.

---

## 13. Local / published parity

- Customer-facing before: **19**  
- Customer-facing after: **19**  
- Canonical `opportunities.json` SHA: **unchanged**  
- Main local GDI page still matches published list.

---

## 14. Regression tests

```bash
node scripts/test-gdi-bethesda-commercial-recovery-8usd-v1.mjs
node scripts/test-gdi-watchlist-finalization-handoff-v1.mjs
```

---

## 15. GitHub PR / main SHA

Filled after merge in this folder’s `00-summary.json` / PR description.

---

## Safety attestations

- Paid spend ≤ $8.00 (**$2.40 used**)  
- No automatic customer publication  
- No destructive store / Airtable writes  
- Existing research IDs preserved  
- Hotel feedback / Decision Events untouched  
- No parallel GDI engine  
- No fabricated demand; no forced Sales Ready  
