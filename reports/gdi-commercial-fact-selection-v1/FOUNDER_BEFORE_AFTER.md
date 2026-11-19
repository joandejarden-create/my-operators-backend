# GDI Commercial Fact Selection V1 — founder before/after

**Amendment:** complete source retention + section-level citations.  
Fixed clock: 2026-11-19. Paid Exa: **0**. Airtable writes: **0**.

## Objective

Commercially important verified findings must not disappear between archived research and the customer opportunity detail — and every selected fact must keep recoverable source relationships.

## Independent quality gates

| Sample | 1 Source retention | 2 Fact→source | 3 Section placement | 4 L1 selection | 5 L1+L2 retention | 6 Unsupported/misattr | 7 Confidential leak | 8 Persistence |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| pdac | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |
| nice | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |
| nrc-ric | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |
| imc-construction | 100 | 100 | 100 | 100 | 100 | 2 | 0 | NOT_CERTIFIED |
| owwa | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |

> A 100% fact-retention score is **not** sufficient if source URLs / sourceIds are missing.

### Persistence / read-back (gate 8) — gap report

Slice 2A slim inventory keep **URL evidence rows**, not the full `commercialSourceRegister` / `sectionCitations` / `fact.sourceIds` graph.

- commercialSourceRegister / sectionCitations / fact.sourceIds are not in Slice 2A slim allowlist
- Slim may keep evidence URLs only (title/publisher/fact links may drop)
- Full register must live in durable research store + durableRef until slim contract is extended (founder approval)

Durable catalogs remain referenced via `commercialFactCatalog.durableRef`. Full persistence certification requires a founder-approved slim/store contract extension — **not claimed here**.

## Rendered drawer sample — PDAC (contextual citations + All Sources)

```html
<article class="gdi-drawer-render" data-opportunity="rbc_gdi_disc_45447a18d2d0e412">
  <section data-section-id="what_is_happening"><h3>What is happening</h3>
    <p>PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radisson Blu Toronto Downtown as an official hotel partner with promo rate CAD $449 and reference code for direct booking (cutoff 2 Feb 2027). Limited availability remains on select nights. PDAC does not use 3rd party housing bureaus; book directly with hotels. Edition-specific PDAC 2027 Hotel Partners page publishes direct hotel booking routes and promotional rates/blocks. Exact instruction: “Book directly with your hotel(s) of choice using the links we provide above.” Exact responsibility statement: “PDAC does not manage hotel reservations and does NOT use 3rd party housing</p>
    <ul class="gdi-section-citations" data-section-citations="demand_context"><li><a href="https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027">PDAC 2027 Convention</a></li><li><a href="https://pdac.ca/about-pdac/staff">PDAC 2027 Convention</a></li></ul><ul class="gdi-section-citations" data-section-citations="competitors_venues"><li><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">PDAC 2027 Convention</a></li></ul>
  </section>
  <section data-section-id="potential_business"><h3>Potential business for this hotel</h3>
    <p>Fill remaining partner-rate inventory; pursue exhibitor/company suites and overflow once partner nights sell; do not confuse with exclusive venue housing.</p>
    
  </section>
  <section data-section-id="why_this_hotel"><h3>Why this hotel</h3>
    <p>157 guestrooms / ~11,000 sq ft / 20 meeting rooms on Queens Quay — realistic for mid-size groups, overflow, and meeting+rooms packages; not exclusive MTCC mega-housing. PDAC official partner list includes this hotel at 8-minute walk to MTCC.</p>
    <ul class="gdi-section-citations" data-section-citations="hotel_relevance"><li><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">PDAC 2027 Convention</a></li></ul>
  </section>
  <section data-section-id="why_act_now"><h3>Why act now</h3>
    <p>Partner promo cutoff 2 Feb 2027; limited availability already flagged on select nights.</p>
    <ul class="gdi-section-citations" data-section-citations="commercial_timing"><li><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">PDAC 2027 Convention</a></li></ul>
  </section>
  <section data-section-id="who_to_contact"><h3>Who to contact</h3>
    <p>Nicole Sampson</p>
    <ul class="gdi-section-citations" data-section-citations="buyer_contacts"><li><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">PDAC 2027 Convention</a></li><li><a href="https://pdac.ca/about-pdac/staff">PDAC 2027 Convention</a></li></ul>
  </section>
  <section data-section-id="recommended_next_step"><h3>Recommended next step</h3>
    <p>Qualify before pursuing — no direct route is published; a Radisson Blu participant/partner listing already exists, so this is account management of an existing listing rather than new prospecting. Start with Nicole Sampson (Senior Director, Convention, Prospectors &amp; Developers Association of Canada) to review how the existing Radisson Blu listing for PDAC 2027 Convention (Mar 7–10, 2027) is performing (pickup, release dates, remaining inventory) and what extra rooms or meeting space the organizer needs. convention@pdac.ca is a published organizational route on staff materials</p>
    <ul class="gdi-section-citations" data-section-citations="recommended_next_step"><li><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">PDAC 2027 Convention</a></li><li><a href="https://pdac.ca/about-pdac/staff">PDAC 2027 Convention</a></li></ul>
  </section>
  <section data-section-id="supporting_research"><h3>Supporting Research</h3>
    <ul><li>Limited availability remains on select nights — <a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027</a></li></ul>
  </section>
  <section data-section-id="all_sources"><h3>All Sources</h3>
    <ul><li><strong>PDAC 2027 Convention</strong> — <a href="https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027">https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027</a> <span class="muted">[demand_context]</span></li><li><strong>PDAC 2027 Convention</strong> — <a href="https://pdac.ca/convention-2027/attending-2027/key-dates-venue-info-2027">https://pdac.ca/convention-2027/attending-2027/key-dates-venue-info-2027</a> <span class="muted">[]</span></li><li><strong>PDAC 2027 Convention</strong> — <a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027</a> <span class="muted">[hotel_relevance, commercial_timing, buyer_contacts, recommended_next_step, competitors_venues, supporting_research]</span></li><li><strong>PDAC 2027 Convention</strong> — <a href="https://pdac.ca/about-pdac/staff">https://pdac.ca/about-pdac/staff</a> <span class="muted">[buyer_contacts, recommended_next_step, demand_context]</span></li></ul>
  </section>
</article>
```

Also written to `RENDERED_DRAWER_PDAC.html`.

## Rendered before → after (by opportunity)

### pdac — PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radis…

**1. Opportunity brief (after)**  
> PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radisson Blu Toronto Downtown as an official hotel partner with promo rate CAD $449 and reference code for direct booking (cutoff 2 Feb 2027). Limited availability remains on select nights. PDAC does not use 3rd party housing bureaus; book directly with hotels. Edition-specific PDAC 2027 Hotel Partners page publishes direct hotel booking routes and promotional rates/blocks. Exact instruction: “Book directly with your hotel(s) of choice using the links we provide above.” Exact responsibility statement: “PDAC does not manage hotel reservations and does NOT use 3rd party housing

**Why this hotel:** 157 guestrooms / ~11,000 sq ft / 20 meeting rooms on Queens Quay — realistic for mid-size groups, overflow, and meeting+rooms packages; not exclusive MTCC mega-housing. PDAC official partner list includes this hotel at 8-minute walk to MTCC.  
**Potential business:** Fill remaining partner-rate inventory; pursue exhibitor/company suites and overflow once partner nights sell; do not confuse with exclusive venue housing.  
**Why act now:** Partner promo cutoff 2 Feb 2027; limited availability already flagged on select nights.  
**Next step:** Qualify before pursuing — no direct route is published; a Radisson Blu participant/partner listing already exists, so this is account management of an existing listing rather than new prospecting. Start with Nicole Sampson (Senior Director, Convention, Prospectors & Developers Association of Canada) to review how the existing Radisson Blu listing for PDAC 2027 Convention (Mar 7–10, 2027) is performing (pickup, release dates, remaining inventory) and what extra rooms or meeting space the organizer needs. convention@pdac.ca is a published organizational route on staff materials  
**Posture:** `LISTED_PARTNER_QUALIFY`

**2. Supporting Research (Level 2)**  
- Limited availability remains on select nights — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(sourceIds: csrc_abe81338_2)_

**3. Section-level citations**  
**Opportunity / demand context**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027 _(facts: pdac-dates-mtcc)_
  - PDAC 2027 Convention — https://pdac.ca/about-pdac/staff _(facts: pdac-nicole-sampson, pdac-convention-inbox)_

**Hotel relevance**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_

**Commercial timing**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_

**Buyer contacts and booking routes**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_
  - PDAC 2027 Convention — https://pdac.ca/about-pdac/staff _(facts: pdac-nicole-sampson, pdac-convention-inbox)_

**Competitors / historical venues**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_

**Recommended next step**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_
  - PDAC 2027 Convention — https://pdac.ca/about-pdac/staff _(facts: pdac-nicole-sampson, pdac-convention-inbox)_

**Supporting Research**
  - PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_

**4. All Sources (deduplicated register)**  
- `csrc_e18ece1b_0` PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027 _(sections: demand_context; facts: pdac-dates-mtcc)_
- `csrc_3dc926bf_1` PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/key-dates-venue-info-2027 _(sections: ; facts: )_
- `csrc_abe81338_2` PDAC 2027 Convention — https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027 _(sections: hotel_relevance, commercial_timing, buyer_contacts, recommended_next_step, competitors_venues, supporting_research; facts: pdac-radisson-partner-rate, pdac-no-housing-bureau, pdac-limited-nights, inline-lodging-direct-book)_
- `csrc_e597816f_3` PDAC 2027 Convention — https://pdac.ca/about-pdac/staff _(sections: buyer_contacts, recommended_next_step, demand_context; facts: pdac-nicole-sampson, pdac-convention-inbox)_

**5. Promoted into Level 1**  
- **pdac-dates-mtcc**: PDAC 2027 runs 7–10 March 2027 at MTCC _(was BRIEF → BRIEF; sources: csrc_e18ece1b_0)_
- **pdac-radisson-partner-rate**: Radisson Blu is an official hotel partner at CAD $449 with 2 Feb 2027 cutoff _(was BRIEF → BRIEF; sources: csrc_abe81338_2)_
- **pdac-no-housing-bureau**: PDAC does not use 3rd party housing bureaus; book directly with hotels _(was SUPPORTING → BRIEF; sources: csrc_abe81338_2)_
- **pdac-nicole-sampson**: Nicole Sampson, Senior Director, Convention is the primary commercial entry _(was BRIEF → BRIEF; sources: csrc_e597816f_3)_
- **pdac-convention-inbox**: convention@pdac.ca is a published organizational route on staff materials _(was SUPPORTING → BRIEF; sources: csrc_e597816f_3)_
- **inline-lodging-direct-book**: Edition-specific PDAC 2027 Hotel Partners page publishes direct hotel booking routes and promotional rates/blocks. Exact instruction: “Book directly with your hotel(s) of choice using the links we provide above.” Exact responsibility statement: “PDAC does not manage hotel reservations and does NOT use 3rd party housing _(was n/a → BRIEF; sources: csrc_abe81338_2)_

**6. Retained supporting only**  
- **pdac-limited-nights**: Limited availability remains on select nights

**7. Missing / unsupported / provenance**  
- **pdac-2027-attendance**: Verified 2027 attendance forecast — absent from research


**Archived research paths:** reports/gdi-radisson-blu-toronto-paid-full-e2e-15usd-v1/raw-runs/cmiC_rbc_gdi_disc_45447a18d2d0e412.json; data/group-demand-intelligence/hotels/recH9h3McgMNFQEla/opportunities.json

### nice — NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; l…

**1. Opportunity brief (after)**  
> NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; location TBA.

**Why this hotel:** Bethesda Marriott inventory (407 guestrooms) and ~25,000 sq ft meeting space is unverified as a host until the NIST / National Initiative for Cybersecurity Education (NICE) venue/RFP path is confirmed. Do not treat an unannounced venue as hotel suitability.  
**Potential business:** Potential hotel business depends on verified peak rooms, meeting agenda, and buyer authority for NIST / National Initiative for Cybersecurity Education (NICE). Public research has not confirmed a contracted room block at Bethesda Marriott. FIU / New America operate conference logistics under NIST award  
**Why act now:** CONTACT NOW: Official NIST page still lists Location to be announced for June 7–9, 2027. Historical attendance (~400–450) is mid-size and consistent with Bethesda Marriott meeting inventory. Lead time ~9 months.  
**Next step:** Contact Karen Wetzel (karen.wetzel@nist.gov) to confirm venue/sourcing status is still open, introduce the hotel, and request RFP / site-visit inclusion.  
**Posture:** `FUTURE_VENUE_OPEN`

**2. Supporting Research (Level 2)**  
- Historical attendance ~400–450 — https://www.nist.gov/news-events/news/2026/08/foundations-future-insights-2026-nice-conference _(sourceIds: csrc_2b4d2103_2)_
- Customer reported hotel added to sourced properties (2026-09-17) _(hotel-private)_

**3. Section-level citations**  
**Opportunity / demand context**
  - FIU NICE conference page — https://it.fiu.edu/national-initiative-for-cybersecurity-education-nice-conference/ _(facts: nice-fiu-ops)_

**Buyer contacts and booking routes**
  - NIST NICE staff — https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff _(facts: nice-karen)_
  - FIU NICE conference page — https://it.fiu.edu/national-initiative-for-cybersecurity-education-nice-conference/ _(facts: nice-fiu-ops)_

**Recommended next step**
  - NIST NICE staff — https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff _(facts: nice-karen)_
  - FIU NICE conference page — https://it.fiu.edu/national-initiative-for-cybersecurity-education-nice-conference/ _(facts: nice-fiu-ops)_

**Supporting Research**
  - NIST NICE 2027 — https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo _(facts: nice-dates)_
  - NIST 2026 NICE recap — https://www.nist.gov/news-events/news/2026/08/foundations-future-insights-2026-nice-conference _(facts: nice-historical-attendance)_
  - FIU NICE conference page — https://it.fiu.edu/national-initiative-for-cybersecurity-education-nice-conference/ _(facts: nice-fiu-ops)_

**4. All Sources (deduplicated register)**  
- `csrc_489035b2_0` NIST NICE 2027 — https://www.nist.gov/news-events/events/18th-annual-nice-conference-and-expo _(sections: supporting_research; facts: nice-dates)_
- `csrc_2b4d2103_2` NIST 2026 NICE recap — https://www.nist.gov/news-events/news/2026/08/foundations-future-insights-2026-nice-conference _(sections: supporting_research; facts: nice-historical-attendance)_
- `csrc_bc5e2ca3_4` NIST 2025 NICE recap — https://www.nist.gov/news-events/news/2025/06/climbing-higher-together-insights-nice-2025-conference _(sections: ; facts: )_
- `csrc_9315680_5` FIU NICE conference page — https://it.fiu.edu/national-initiative-for-cybersecurity-education-nice-conference/ _(sections: demand_context, buyer_contacts, recommended_next_step, supporting_research; facts: nice-fiu-ops)_
- `csrc_6275e90d_6` NIST NICE staff — https://www.nist.gov/itl/applied-cybersecurity/nice/about/meet-staff _(sections: buyer_contacts, recommended_next_step; facts: nice-karen)_

**5. Promoted into Level 1**  
- **nice-dates**: NICE 2027 June 7–9; location TBA _(was BRIEF → BRIEF; sources: csrc_489035b2_0)_
- **nice-karen**: Karen Wetzel karen.wetzel@nist.gov is verified decision-maker contact _(was BRIEF → BRIEF; sources: csrc_6275e90d_6)_
- **nice-fiu-ops**: FIU / New America operate conference logistics under NIST award _(was SUPPORTING → BRIEF; sources: csrc_9315680_5)_

**6. Retained supporting only**  
- **nice-historical-attendance**: Historical attendance ~400–450
- **nice-customer-sourced**: Customer reported hotel added to sourced properties (2026-09-17) _(hotel-private)_

**7. Missing / unsupported / provenance**  
- **nice-2027-city**: Confirmed 2027 host city — absent from research


**Archived research paths:** data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json; reports/group-demand-intelligence/bethesda-nice-commercial-progression-apply.json

### nrc-ric — NRC Regulatory Information Conference is hybrid at NRC HQ in…

**1. Opportunity brief (after)**  
> NRC Regulatory Information Conference is hybrid at NRC HQ in Rockville. Bethesda Marriott is a lodging-adjacent overflow candidate for attendees — not the contracted host venue.

**Why this hotel:** Bethesda Marriott was evaluated against this past cycle. Any lodging adjacency in prior research is historical only — not a current host or overflow commitment.  
**Potential business:** The dated cycle appears past. Do not treat current research as an open booking window; archive learnings and watch for the next published cycle.  
**Why act now:** Published dates are past. Close active pursuit on this cycle; retain contact path for the next edition only.  
**Next step:** Do not pursue this past cycle. Optionally note Lorna Kipfer (RIC Conference Contact — U.S. Nuclear Regulatory Commission) Lorna.Kipfer@nrc.gov for the next published edition; no current RFP action.  
**Posture:** `PAST_CYCLE_DO_NOT_PURSUE`

**2. Supporting Research (Level 2)**  
- RICHelpDesk.Resource@nrc.gov is organizational housing route — https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information _(sourceIds: csrc_bd849d8b_0)_

**3. Section-level citations**  
**Hotel relevance**
  - Regulatory Information Conference — https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information _(facts: nrc-date, nrc-hq-host, nrc-helpdesk)_

**Commercial timing**
  - Regulatory Information Conference — https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information _(facts: nrc-date, nrc-hq-host, nrc-helpdesk)_

**Buyer contacts and booking routes**
  - Regulatory Information Conference — https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information _(facts: nrc-date, nrc-hq-host, nrc-helpdesk)_

**Recommended next step**
  - Regulatory Information Conference — https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information _(facts: nrc-date, nrc-hq-host, nrc-helpdesk)_

**4. All Sources (deduplicated register)**  
- `csrc_bd849d8b_0` Regulatory Information Conference — https://www.nrc.gov/public-meetings-hearings/conferences-symposia/regulatory-information-conference/hotel-information _(sections: commercial_timing, recommended_next_step, hotel_relevance, buyer_contacts; facts: nrc-date, nrc-hq-host, nrc-helpdesk)_
- `csrc_2236835b_1` Prior discovery URL — https://www.nrc.gov/public-involve/conference-symposia/ric/hotels _(sections: ; facts: )_
- `csrc_cac51137_3` Contact — Lorna Kipfer — https://www.nrc.gov/public-involve/conferences _(sections: buyer_contacts, recommended_next_step; facts: )_

**5. Promoted into Level 1**  
- **nrc-date**: RIC dated 2026-09-30 — past cycle; no current open pursuit _(was BRIEF → BRIEF; sources: csrc_bd849d8b_0)_
- **nrc-hq-host**: Hybrid at NRC HQ Rockville; Bethesda is overflow-adjacent not contracted host _(was BRIEF → BRIEF; sources: csrc_bd849d8b_0)_

**6. Retained supporting only**  
- **nrc-helpdesk**: RICHelpDesk.Resource@nrc.gov is organizational housing route

**7. Missing / unsupported / provenance**  
- **nrc-peak-rooms**: Verified peak rooms — absent from research


**Archived research paths:** data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json

### imc-construction — IMC Construction: Documented local expansion / growth activi…

**1. Opportunity brief (after)**  
> IMC Construction: Documented local expansion / growth activity — opened a Bethesda regional hub to expand into the Washington D.C. Original public date: 2026-07-21.

**Why this hotel:** Bethesda Marriott sits in the same commercial market (Bethesda) as the documented IMC Construction activity. No confirmed room block, RFP, or preferred-hotel agreement is established from public research alone.  
**Potential business:** Local office / workspace footprint was publicly announced. Plausible hotel demand (inferred, not verified): Visiting leadership, multi-office collaborators, and customer/partner meetings near the local site are plausible transient demand.  
**Why act now:** Establish the corporate relationship while local expansion/training/project activity is observable and lodging patterns may still be open.  
**Next step:** Contact Dan Baker (Vice President (DMV expansion lead)) via info@imcconstruction.com to ask who coordinates visiting-staff lodging and local meetings for the Bethesda / DMV expansion — do not pitch a conference RFP without evidenced group demand.  
**Posture:** `SCALING_ACCOUNT_QUALIFY`

**2. Supporting Research (Level 2)**  
- info@imcconstruction.com is public organizational route — https://imcconstruction.com/imc-construction-expands-into-the-washington-d-c-region/ _(sourceIds: csrc_4a5bb6c2_0)_
- Dan Baker named as DMV expansion lead (email not public) — https://imcconstruction.com/imc-construction-expands-into-the-washington-d-c-region/ _(sourceIds: csrc_4a5bb6c2_0)_
- IMC Construction opened a Bethesda regional hub to expand into the Washington D.C. / DMV market (company announcement)
- Named contact: Dan Baker — Vice President (DMV expansion lead) (NAMED_POTENTIAL_INFLUENCER)

**3. Section-level citations**  
**Opportunity / demand context**
  - Growth / operations evidence — https://imcconstruction.com/imc-construction-expands-into-the-washington-d-c-region/ _(facts: imc-bethesda-hub, imc-info-route, imc-dan-baker)_

**Buyer contacts and booking routes**
  - Growth / operations evidence — https://imcconstruction.com/imc-construction-expands-into-the-washington-d-c-region/ _(facts: imc-bethesda-hub, imc-info-route, imc-dan-baker)_

**Recommended next step**
  - Growth / operations evidence — https://imcconstruction.com/imc-construction-expands-into-the-washington-d-c-region/ _(facts: imc-bethesda-hub, imc-info-route, imc-dan-baker)_

**4. All Sources (deduplicated register)**  
- `csrc_4a5bb6c2_0` Growth / operations evidence — https://imcconstruction.com/imc-construction-expands-into-the-washington-d-c-region/ _(sections: demand_context, buyer_contacts, recommended_next_step; facts: imc-bethesda-hub, imc-info-route, imc-dan-baker)_
- `csrc_55185ff8_1` Growth / operations evidence — https://www.connectcre.com/stories/minority-owned-construction-firm-imc-construction-expands-to-dc/ _(sections: ; facts: )_

**5. Promoted into Level 1**  
- **imc-bethesda-hub**: IMC opened a Bethesda regional hub to expand into Washington D.C. (announced 2026-07-21) _(was BRIEF → BRIEF; sources: csrc_4a5bb6c2_0)_

**6. Retained supporting only**  
- **imc-info-route**: info@imcconstruction.com is public organizational route
- **imc-dan-baker**: Dan Baker named as DMV expansion lead (email not public)
- **established-imc-construction-opened-a-bethesda-regio**: IMC Construction opened a Bethesda regional hub to expand into the Washington D.C. / DMV market (company announcement)
- **established-named-contact-dan-baker-vice-presiden**: Named contact: Dan Baker — Vice President (DMV expansion lead) (NAMED_POTENTIAL_INFLUENCER)

**7. Missing / unsupported / provenance**  
- **imc-room-block**: Confirmed hotel room block or preferred agreement — absent from research
- DEFECT `PROVENANCE_INCOMPLETE`: established-imc-construction-opened-a-bethesda-regio
- DEFECT `PROVENANCE_INCOMPLETE`: established-named-contact-dan-baker-vice-presiden

**Archived research paths:** data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunities.json

### owwa — 2027 Ontario Water & Wastewater Conference & Trade Show is c…

**1. Opportunity brief (after)**  
> 2027 Ontario Water & Wastewater Conference & Trade Show is committed at Hotel X Toronto; Sheraton Centre Toronto; Hyatt Regency Toronto; Le Méridien Toronto Pinnacle Hotel (May 10, 2027 – May 13, 2027). The principal housing is not available to Radisson Blu Toronto Downtown. Venue is Enercare Centre, Exhibition Place. Hotel reservations open October 20, 2026 (announced opening; past as of mid-November 2026 research — do not treat as a future deadline)

**Why this hotel:** Conditional supplementary delegate/speaker/exhibitor or executive lodging; no published room volume or overflow requirement. Radisson Blu Toronto Downtown cannot realistically win the committed 2027 principal housing; Convention-scale event (~2000 attendees); realistic role for a 157-room Harbourfront hotel is delegate/speaker overflow, VIP lodging and side functions — not primary housing.  
**Potential business:** Future-cycle account development and/or separately evidenced overflow only — not the committed principal housing.  
**Why act now:** Competitor commitment confirmed at Hotel X Toronto; Sheraton Centre Toronto; Hyatt Regency Toronto; Le Méridien Toronto Pinnacle Hotel; develop the organizer relationship for a future cycle or residual overflow lodging.  
**Next step:** Contact Sam Petryna (Conference & Sales Manager, OWWA) to establish whether overflow accommodation or a secondary room block is needed for Radisson Blu Toronto Downtown. Competitor booking is confirmed at Hotel X Toronto; Sheraton Centre Toronto; Hyatt Regency Toronto; Le Méridien Toronto Pinnacle Hotel — do not request an RFP as host. spetryna@owwa.ca verified on staff page (Exa)  
**Posture:** `COMPETITOR_PLACED_OVERFLOW_ONLY`

**2. Supporting Research (Level 2)**  
_None selected for Level 2 surface (catalog may still retain OMIT_MISSING)._

**3. Section-level citations**  
**Opportunity / demand context**
  - 2027 Ontario Water & Wastewater Conference & Trade Show — https://owwa.ca/event/ontarios-water-wastewater-conference-exposition-2027/ _(facts: owwa-dates, owwa-enercare)_
  - Contact — Sam Petryna — https://owwa.ca/about/board-and-staff/ _(facts: owwa-sam, owwa-sam-email)_

**Hotel relevance**
  - 2027 Ontario Water & Wastewater Conference & Trade Show — https://owwa.ca/event/ontarios-water-wastewater-conference-exposition-2027/ _(facts: owwa-dates, owwa-enercare)_

**Commercial timing**
  - 2027 Ontario Water & Wastewater Conference & Trade Show — https://www.weao.org/2027-joint-conference-present-partner-and-connect/ _(facts: owwa-four-hotels, owwa-oct20)_

**Buyer contacts and booking routes**
  - Contact — Sam Petryna — https://owwa.ca/about/board-and-staff/ _(facts: owwa-sam, owwa-sam-email)_

**Competitors / historical venues**
  - 2027 Ontario Water & Wastewater Conference & Trade Show — https://www.weao.org/2027-joint-conference-present-partner-and-connect/ _(facts: owwa-four-hotels, owwa-oct20)_

**Recommended next step**
  - 2027 Ontario Water & Wastewater Conference & Trade Show — https://www.weao.org/2027-joint-conference-present-partner-and-connect/ _(facts: owwa-four-hotels, owwa-oct20)_
  - Contact — Sam Petryna — https://owwa.ca/about/board-and-staff/ _(facts: owwa-sam, owwa-sam-email)_

**4. All Sources (deduplicated register)**  
- `csrc_f392d42_0` 2027 Ontario Water & Wastewater Conference & Trade Show — https://www.weao.org/2027-joint-conference-present-partner-and-connect/ _(sections: competitors_venues, commercial_timing, recommended_next_step; facts: owwa-four-hotels, owwa-oct20)_
- `csrc_962e243d_1` 2027 Ontario Water & Wastewater Conference & Trade Show — https://owwa.ca/events/ _(sections: ; facts: )_
- `csrc_7e7e5590_2` 2027 Ontario Water & Wastewater Conference & Trade Show — https://owwa.ca/event/ontarios-water-wastewater-conference-exposition-2027/ _(sections: demand_context, hotel_relevance; facts: owwa-dates, owwa-enercare)_
- `csrc_c239c4d_3` 2027 Ontario Water & Wastewater Conference & Trade Show — https://www.weao.org/wp-content/uploads/2026/08/2027-Joint-Conference-Exhibitor-Prospectus.pdf _(sections: ; facts: )_
- `csrc_800f31d_4` Contact — Sam Petryna — https://owwa.ca/about/board-and-staff/ _(sections: buyer_contacts, recommended_next_step, demand_context; facts: owwa-sam, owwa-sam-email)_

**5. Promoted into Level 1**  
- **owwa-dates**: May 10–13, 2027 conference dates _(was BRIEF → BRIEF; sources: csrc_7e7e5590_2)_
- **owwa-four-hotels**: Committed blocks at Hotel X, Sheraton Centre, Hyatt Regency, Le Méridien Pinnacle _(was BRIEF → BRIEF; sources: csrc_f392d42_0)_
- **owwa-enercare**: Venue is Enercare Centre, Exhibition Place _(was SUPPORTING → BRIEF; sources: csrc_7e7e5590_2)_
- **owwa-oct20**: Hotel reservations open October 20, 2026 (announced opening; past as of mid-November 2026 research — do not treat as a future deadline) _(was SUPPORTING → BRIEF; sources: csrc_f392d42_0)_
- **owwa-sam**: Sam Petryna Conference & Sales Manager is commercial entry _(was BRIEF → BRIEF; sources: csrc_800f31d_4)_
- **owwa-sam-email**: spetryna@owwa.ca verified on staff page (Exa) _(was SUPPORTING → BRIEF; sources: csrc_800f31d_4)_

**6. Retained supporting only**  
_None_

**7. Missing / unsupported / provenance**  
- **owwa-live-pickup**: Live 2027 pickup / rates — absent from research


**Archived research paths:** reports/gdi-radisson-blu-toronto-paid-full-e2e-15usd-v1/raw-runs/cmiC_rbc_gdi_disc_2d592c1aec6a782f.json; data/group-demand-intelligence/hotels/recH9h3McgMNFQEla/opportunities.json


## Regression highlights
- **PDAC:** no-housing-bureau / direct book → L1 with sourceIds; Nicole Sampson + convention route cited; truncated lodging repaired without inventing the ending; section citations + All Sources rendered.
- **NICE:** FIU/New America material; hotel-private sourced-properties stays out of shared L1.
- **OWWA:** Enercare + October 20 **2026** (past milestone, not future deadline); Sam Petryna retained; no unsupported overflow-volume claim.
- **NRC RIC:** past-cycle posture; no open pursuit / no credible-host invention; no active current-cycle RFP implication.
- **IMC:** expansion hub retained; confirmed room block remains ABSENT (not invented).

Paid/Airtable/publication: 0 · Persistence certification: **NOT claimed**
