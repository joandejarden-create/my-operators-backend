# GDI Commercial Fact Selection V1 — founder before/after

**Amendment:** consolidated **Sources (N)** buttons (structural parity with existing drawer).  
Fixed clock: 2026-11-19. Paid Exa: **0**. Airtable writes: **0**.

## Final drawer parity (component vs origin/main)

| Check | Result |
|---|---|
| Association section hierarchy equal to `origin/main` | **PASS** |
| Section IDs | `opportunity_overview → what_is_happening → potential_business → why_this_hotel → why_act_now → facts_and_uncertainties → who_to_contact → recommended_next_step → supporting_evidence → hotel_validation` |
| Visible citation lists | absent |
| Prominent All Sources | absent |
| Sources open by default | collapsed (PASS) |
| Nested Sources under Supporting Research | absent (PASS) |
| Authenticated browser smoke (`localhost:8080`) | **BLOCKED** — host not reachable; DOM/parity regressions used (not visual certification) |

Persistence / read-back: **NOT_CERTIFIED** (slim URL rows only; full register via durableRef until Track A persistence certification).

## UI correction

- Removed visible section citation lists and prominent All Sources block from the customer drawer.
- One `Sources (N)` expandable control per applicable section (existing `gdi-inline-details` convention), **collapsed by default**.
- N = unique sources for that section (URL-deduped).
- Descriptive titles derived from path when stored title was a generic opportunity name.
- Supporting Research folded into existing Supporting evidence as expandable detail (no new top-level section).
- Full `allSources` / `sourceIds` register retained in underlying data (not customer-primary).

- PDAC next step: published `convention@pdac.ca` retained; does **not** claim that no contact route is published.

## Independent quality gates

| Sample | 1 Source retention | 2 Fact→source | 3 Section placement | 4 L1 selection | 5 L1+L2 retention | 6 Unsupported/misattr | 7 Confidential leak | 8 Persistence |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| pdac | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |
| nice | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |
| nrc-ric | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |
| imc-construction | 100 | 100 | 100 | 100 | 100 | 2 | 0 | NOT_CERTIFIED |
| owwa | 100 | 100 | 100 | 100 | 100 | 0 | 0 | NOT_CERTIFIED |

### Persistence (gate 8)

- commercialSourceRegister / sectionCitations / fact.sourceIds are not in Slice 2A slim allowlist
- Slim may keep evidence URLs only (title/publisher/fact links may drop)
- Full register must live in durable research store + durableRef until slim contract is extended (founder approval)

## Structural parity demo

See `RENDERED_DRAWER_SOURCES_BUTTONS.html` (PDAC / NICE / OWWA side-by-side before note + after with Sources expanded).

### PDAC after (excerpt)

```html
<article class="gdi-drawer-render" data-opportunity="rbc_gdi_disc_45447a18d2d0e412">
  <section data-section-id="opportunity_overview"><h3>Opportunity overview</h3><p>PDAC 2027 Convention</p></section>
  <section data-section-id="what_is_happening"><h3>What is happening</h3>
    <p>PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radisson Blu Toronto Downtown as an official hotel partner with promo rate CAD $449 and reference code for direct booking (cutoff 2 Feb 2027). Limited availability remains on select nights. PDAC does not use 3rd party housing bureaus; book directly with hotels</p>
    <details class="gdi-inline-details gdi-section-sources"><summary>Sources (2)</summary><ul class="gdi-evidence-default"><li class="gdi-evidence-item"><a href="https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027">Frequently Asked Questions 2027</a></li><li class="gdi-evidence-item"><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">Hotels 2027</a></li></ul></details>
  </section>
  <section data-section-id="potential_business"><h3>Potential business for this hotel</h3>
    <p>Fill remaining partner-rate inventory; pursue exhibitor/company suites and overflow once partner nights sell; do not confuse with exclusive venue housing.</p>
    
  </section>
  <section data-section-id="why_this_hotel"><h3>Why this hotel</h3>
    <p>157 guestrooms / ~11,000 sq ft / 20 meeting rooms on Queens Quay — realistic for mid-size groups, overflow, and meeting+rooms packages; not exclusive MTCC mega-housing. PDAC official partner list includes this hotel at 8-minute walk to MTCC.</p>
    <details class="gdi-inline-details gdi-section-sources"><summary>Sources (1)</summary><ul class="gdi-evidence-default"><li class="gdi-evidence-item"><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">Hotels 2027</a></li></ul></details>
  </section>
  <section data-section-id="why_act_now"><h3>Why act now</h3>
    <p>Partner promo cutoff 2 Feb 2027; limited availability already flagged on select nights.</p>
    <details class="gdi-inline-details gdi-section-sources"><summary>Sources (1)</summary><ul class="gdi-evidence-default"><li class="gdi-evidence-item"><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">Hotels 2027</a></li></ul></details>
  </section>
  <section data-section-id="who_to_contact"><h3>Who to contact</h3>
    <p>Nicole Sampson</p>
    <details class="gdi-inline-details gdi-section-sources"><summary>Sources (1)</summary><ul class="gdi-evidence-default"><li class="gdi-evidence-item"><a href="https://pdac.ca/about-pdac/staff">Staff</a></li></ul></details>
  </section>
  <section data-section-id="recommended_next_step"><h3>Recommended next step</h3>
    <p>Qualify before pursuing — a Radisson Blu participant/partner listing already exists, so this is account management of an existing listing rather than new prospecting. Use the published organizational route convention@pdac.ca and start with Nicole Sampson (Senior Director, Convention, Prospectors &amp; Developers Association of Canada) to review how the existing Radisson Blu listing for PDAC 2027 Convention (Mar 7–10, 2027) is performing (pickup, release dates, remaining inventory) and what extra rooms or meeting space the organizer needs. A verified personal email for Nicole Sampson was not published on the staff materials reviewed.</p>
    <details class="gdi-inline-details gdi-section-sources"><summary>Sources (2)</summary><ul class="gdi-evidence-default"><li class="gdi-evidence-item"><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">Hotels 2027</a></li><li class="gdi-evidence-item"><a href="https://pdac.ca/about-pdac/staff">Staff</a></li></ul></details>
  </section>
  <section data-section-id="supporting_evidence"><h3>Supporting evidence</h3>
    <ul><li><a href="https://pdac.ca/convention-2027/attending-2027/frequently-asked-questions-2027">Frequently Asked Questions 2027</a></li><li><a href="https://pdac.ca/convention-2027/attending-2027/key-dates-venue-info-2027">Key Dates Venue Info 2027</a></li><li><a href="https://pdac.ca/convention-2027/attending-2027/accommodation-and-travel-2027/hotels-2027">Hotels 2027</a></li><li><a href="https://pdac.ca/about-pdac/staff">Staff</a></li></ul>
    <details class="gdi-research-detail-expand"><summary>Supporting Research (1)</summary><ul><li>Limited availability remains on select nights</li></ul></details>
  </section>
</article>
```

## Narrative cleanup

### pdac

**Before whatIsHappening:** PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radisson Blu Toronto Downtown as an official hotel partner with promo rate CAD $449 and reference code for direct booking (cutoff 2 Feb 2027). Limited availability remains on select nights.

**After whatIsHappening:** PDAC 2027 Convention (7–10 March 2027, MTCC) publishes Radisson Blu Toronto Downtown as an official hotel partner with promo rate CAD $449 and reference code for direct booking (cutoff 2 Feb 2027). Limited availability remains on select nights. PDAC does not use 3rd party housing bureaus; book directly with hotels

**Sources buttons (unique counts):** 
- What is happening: Sources (2) — Frequently Asked Questions 2027; Hotels 2027
- Potential business for this hotel: Sources (0) _(hidden — empty)_
- Why this hotel: Sources (1) — Hotels 2027
- Why act now: Sources (1) — Hotels 2027
- Who to contact: Sources (1) — Staff
- Recommended next step: Sources (2) — Hotels 2027; Staff

**Underlying register size:** 4 (not shown as primary All Sources list)

### nice

**Before whatIsHappening:** NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; location TBA.

**After whatIsHappening:** NIST NICE 18th Annual Conference and Expo, June 7–9, 2027; location TBA.

**Sources buttons (unique counts):** 
- What is happening: Sources (1) — FIU NICE conference page
- Potential business for this hotel: Sources (0) _(hidden — empty)_
- Why this hotel: Sources (0) _(hidden — empty)_
- Why act now: Sources (0) _(hidden — empty)_
- Who to contact: Sources (2) — NIST NICE staff; FIU NICE conference page
- Recommended next step: Sources (2) — NIST NICE staff; FIU NICE conference page

**Underlying register size:** 5 (not shown as primary All Sources list)

### nrc-ric

**Before whatIsHappening:** NRC Regulatory Information Conference is hybrid at NRC HQ in Rockville. Bethesda Marriott is a lodging-adjacent overflow candidate for attendees — not the contracted host venue.

**After whatIsHappening:** NRC Regulatory Information Conference is hybrid at NRC HQ in Rockville. Bethesda Marriott is a lodging-adjacent overflow candidate for attendees — not the contracted host venue.

**Sources buttons (unique counts):** 
- What is happening: Sources (0) _(hidden — empty)_
- Potential business for this hotel: Sources (0) _(hidden — empty)_
- Why this hotel: Sources (1) — Hotel Information
- Why act now: Sources (1) — Hotel Information
- Who to contact: Sources (1) — Hotel Information
- Recommended next step: Sources (1) — Hotel Information

**Underlying register size:** 3 (not shown as primary All Sources list)

### imc-construction

**Before whatIsHappening:** IMC Construction: Documented local expansion / growth activity — opened a Bethesda regional hub to expand into the Washington D.C. Original public date: 2026-07-21.

**After whatIsHappening:** IMC Construction: Documented local expansion / growth activity — opened a Bethesda regional hub to expand into the Washington D.C. Original public date: 2026-07-21.

**Sources buttons (unique counts):** 
- What is happening: Sources (1) — Growth / operations evidence
- Potential business for this hotel: Sources (0) _(hidden — empty)_
- Why this hotel: Sources (0) _(hidden — empty)_
- Why act now: Sources (0) _(hidden — empty)_
- Who to contact: Sources (1) — Growth / operations evidence
- Recommended next step: Sources (1) — Growth / operations evidence

**Underlying register size:** 2 (not shown as primary All Sources list)

### owwa

**Before whatIsHappening:** 2027 Ontario Water & Wastewater Conference & Trade Show is committed at Hotel X Toronto; Sheraton Centre Toronto; Hyatt Regency Toronto; Le Méridien Toronto Pinnacle Hotel (May 10, 2027 – May 13, 2027). The principal housing is not available to Radisson Blu Toronto Downtown.

**After whatIsHappening:** 2027 Ontario Water & Wastewater Conference & Trade Show is committed at Hotel X Toronto; Sheraton Centre Toronto; Hyatt Regency Toronto; Le Méridien Toronto Pinnacle Hotel (May 10, 2027 – May 13, 2027). The principal housing is not available to Radisson Blu Toronto Downtown. Venue is Enercare Centre, Exhibition Place. Hotel reservations open October 20, 2026 (announced opening; past as of mid-November 2026 research — do not treat as a future deadline)

**Sources buttons (unique counts):** 
- What is happening: Sources (3) — Ontarios Water Wastewater Conference Exposition 2027; Board And Staff; 2027 Joint Conference Present Partner And Connect
- Potential business for this hotel: Sources (0) _(hidden — empty)_
- Why this hotel: Sources (1) — Ontarios Water Wastewater Conference Exposition 2027
- Why act now: Sources (1) — 2027 Joint Conference Present Partner And Connect
- Who to contact: Sources (1) — Board And Staff
- Recommended next step: Sources (2) — 2027 Joint Conference Present Partner And Connect; Board And Staff

**Underlying register size:** 5 (not shown as primary All Sources list)


## Regression highlights
- **PDAC:** one housing-bureau/direct-book statement; no truncated lodging append; Sources buttons with path-based titles (e.g. Hotels 2027, Staff).
- **NICE / OWWA / NRC / IMC:** fact selection + source retention preserved; hotel-private not in shared L1.
- Drawer section hierarchy unchanged (overview → what → potential → why hotel → why now → facts → contact → next → supporting evidence → validation).

Paid/Airtable/publication: 0 · Persistence certification: **NOT claimed**
