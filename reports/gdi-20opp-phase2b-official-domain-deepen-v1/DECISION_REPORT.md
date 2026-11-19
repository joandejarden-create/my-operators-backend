# GDI Phase 2B — Targeted Official-Domain Deepen — Decision Report

**Decision: STOP**  
**Actual paid spend: $0**  
**Executed on:** `ws/gdi` branch `research/gdi-20opp-phase2b-official-domain-deepen-v1`  
**Cases:** CMAA San Diego (`gdi_lmc_00562bf81b1e1957`), DDAA 2027 (`gdi_opp_2027_ddaa_annual_conference_8`) only

## Aggregate

| Metric | Value |
|--------|-------|
| Official-page retrievals | 24 |
| Commercially useful pages (noise-filtered) | 9 |
| New verified named commercially relevant contacts | **0** |
| New public emails paired to names/roles | **0** |
| Confirmed hotel-demand upgrades | **0** |
| Eligibility upgrades | **0** |
| False SALES_READY promotions | **0** |
| Paid USD | **0** |

## Case 1 — CMAA San Diego / Wyndham San Diego Bayside

| Field | Finding |
|-------|---------|
| Opportunity ID | `gdi_lmc_00562bf81b1e1957` |
| Hotel | Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`) |
| Qualification before | RESEARCH_CONTINUATION · frozen `roomDemandStatus=NOT_REQUIRED` · awards banquet historically at **Marriott Marquis** |
| Qualification after | RESEARCH_CONTINUATION (unchanged) |
| New verified facts | Live reconfirmation that chapter meeting pages reference **Marriott Marquis**; chapter site remains StarChapter **preview mode** (unpublished design) |
| New verified named contacts | **None** — board page yields orphan mailto/emails in HTML without extractable name+title pairs in usable text |
| Direct buyers vs entry points | No extractable named buyer; org emails without names are **not** treated as verified commercial contacts |
| Lodging / meetings demand | Meetings/catering relationship inferred; guestrooms **not required** per frozen baseline; primary awards banquet **already placed at competitor** |
| Timing | Standing local relationship (not a dated hotel block) |
| Already-booked / competitive share | **Yes** — Marriott Marquis (frozen + live) |
| Remaining gaps | Named chapter event planner with public role evidence; any Wyndham-specific open catering RFPs |
| Retrieval failures | 0 |
| Paid | $0 |

**Demand thesis before:** recurring awards banquet / waterfront meetings fit; overnight needs not stated.  
**Demand thesis after:** unchanged commercially — competitor venue evidence strengthened, not hotel-demand confidence for Wyndham guestrooms.

## Case 2 — DDAA 2027 / Bethesda Marriott

| Field | Finding |
|-------|---------|
| Opportunity ID | `gdi_opp_2027_ddaa_annual_conference_8` |
| Hotel | Bethesda Marriott (`recLuxvwwxID7U2B8`) |
| Qualification before | RESEARCH_CONTINUATION · `TOO_EARLY` · location ESTIMATED Bethesda · room demand UNKNOWN |
| Qualification after | RESEARCH_CONTINUATION (unchanged) |
| New verified facts | Official 2027 conference URL remains live; board/contact/committees pages fetched; **no hotel block / housing / host-hotel language** on conference page |
| New verified named contacts | **None** — board pages expose member org emails without reliable name+role pairing in static text; `info@appalachiandevelopment.org` is org route only |
| Prior contact (held baseline) | Tony White / Director of Outreach — still unverified email; source was a **2023** housing-program page, not 2027 conference lodging |
| Lodging demand | **Not confirmed** — conference page thin (~1.2k chars of mostly chrome); no accommodation requirement published |
| Timing | 2027-06-01 (still early; lodging unpublished) |
| Already-booked | No host hotel named; not a competitive-share confirmation |
| Remaining gaps | Published housing/hotel block; city/venue confirmation; named conference housing/meetings contact on official pages |
| Retrieval failures | 2× 404 (`/about-us`, `/events/annual-conference`) |
| Paid | $0 |

## Evidence classification

| Kind | CMAA | DDAA |
|------|------|------|
| Genuinely new verified | Competitor venue reconfirmed on live meeting pages | Event page still live; no lodging facts |
| Previously known | Marriott Marquis awards banquet; NOT_REQUIRED rooms | TOO_EARLY; estimated Bethesda; Tony White unverified |
| Inferred demand | Local meetings/catering continues | Conference will happen in 2027 |
| Speculative | Wyndham wins banquet overnight | Bethesda Marriott captures room block |
| Orphan public emails (not named contacts) | Present on board.php | Present on board/committees; info@ org inbox |

## What limited us (exact bottleneck)

Not “not enough URLs.” After 24 same-domain retrievals:

1. **Genuine demand absence / wrong demand type (CMAA):** primary banquet already at Marriott Marquis; room demand flagged NOT_REQUIRED.
2. **Timing / unpublished lodging (DDAA):** TOO_EARLY remains correct — no housing publication to raise demand confidence.
3. **Contact extraction gap on public pages:** StarChapter preview shells and thin WordPress text do not expose name+title+mailto triples that ORS public buyer intelligence can verify. Orphan emails without names were **not** promoted as verified named contacts (no fabrication).

**Capability needed (if revisited later):** JS-rendered board/directory extraction that pairs **visible** name+title with **on-page** mailto only — still would not fix already-booked CMAA banquet or unpublished DDAA lodging.

## Decision vs criteria

| Criterion | Result |
|-----------|--------|
| A. Commercial demand confidence improved? | **No** |
| B. Actionable verified named contact identified? | **No** |
| Unsupported SALES_READY? | **No** |

### STOP

Further official-domain URL expansion on these two allowlists is unproductive. Do not expand to the other 15 or authorize paid providers without a separate founder decision keyed to a new capability (rendered contact pairing) or new published lodging evidence.
