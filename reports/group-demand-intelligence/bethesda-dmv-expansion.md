# Bethesda GDI — DMV Expansion Research Pass

**Date:** 2026-09-13  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Mode:** MODE B — FINISH / RECOVER (research coverage, not reclassification)  
**Webhound spend this pass:** **$0** (prior pilot hard cap already exhausted at **$15 / $15**)

---

## Verdict

# PARTIAL GO

The research universe **did expand beyond Montgomery County** with commercially credible DMV Competitive and DMV Stretch opportunities, each carrying a **Bethesda Win Thesis** and opportunity-market competitors.

It is not a full GO for “DMV coverage complete” because:

- Discovery used **L3 standard web only** (no new Webhound).
- Competitive count is solid but not deep across every submarket (Fairfax/Tysons lodging-heavy annuals remain thin).
- Several Stretch rows are honest **overflow/watch** plays, not primary host bids.

---

## Before (prior 18 qualified)

| Demand Territory | Qualified count |
|------------------|----------------:|
| Bethesda / Montgomery Core | **15** |
| North DC / Medical Corridor | **3** |
| DMV Competitive | **0** |
| DMV Stretch | **0** |

Qualified total: **18**  
Prior DMV Competitive/Stretch tags existed only on **disqualified** contracted downtown venues.

---

## Research performed (new source pools)

Searched deliberately outside Bethesda/NIH/Montgomery-only feeds:

| Geography / pool | Sources used |
|------------------|--------------|
| Washington, DC | Destination DC / ASAE press; AAD locations; ECS 251; WEWCC event directories; ASSA; TEI; NAEYC housing |
| Arlington / Crystal City | NAR GAD Institute; NADO Washington Conference 2026–2028; CARH; ASOR; AAMVA; NCCAN; NPA; DAV |
| Alexandria | Alexandria Soccer Kickoff stay-to-play |
| Fairfax / Tysons | ACG Next (day meeting — rejected); Tysons conference-center news (context only) |
| Loudoun / Dulles | Loudoun Soccer College Showcase + HBC housing |
| Prince George's / College Park | BEBPA USB 2027; Sacred Space (rejected — host locked); UMD hotel patterns |
| National Harbor | NASPA Gaylord package (rejected as primary) |

**Allocation of discovery effort:** heavily weighted to DMV Competitive + Stretch pools. Final qualification did **not** force weak rows to hit quotas.

**Webhound recommendation (not spent):** optional **+$5 to +$10** for deeper association calendars / housing-bureau prospectuses (Arlington CVB, Visit Fairfax, Events DC future books). Not required to validate that coverage expanded.

---

## After (this pass)

| Demand Territory | Qualified count |
|------------------|----------------:|
| Bethesda / Montgomery Core | **15** |
| North DC / Medical Corridor | **3** |
| DMV Competitive | **5** |
| DMV Stretch | **6** |

Qualified total: **29** (within 20–30 target band)  
New opportunities added: **11**  
Rejected researched examples retained for audit: **12**

Priority mix after merge: High **5** · Medium **13** · Watchlist **11** · Disqualified **9**

---

## New DMV opportunities (not in prior Bethesda-centric set)

### DMV Competitive

| Event | Org | Geography | Fit / Conf | Bethesda Win Thesis | Why Now | Contact | Likely competitors |
|-------|-----|-----------|------------|---------------------|---------|---------|-------------------|
| NAR GAD Institute 2027 | NAR | Arlington, VA — hotel TBA | Med / ~high | Arlington destination but hotel unnamed; Bethesda can compete on parking, Metro Red Line to NAR DC office / Hill, and suburban full-service value vs Crystal City rates | Contact now | GADInst@nar.realtor · Jami Sims | Crystal Gateway Marriott; Hilton Arlington; Ritz Pentagon City |
| NADO & DDAA WashCon 2028 | NADO / DDAA | Arlington — hotel TBA | Med | Recurring Crystal Gateway pattern; 2026 block sold out with no official overflow — 2028 hotel not yet named | Qualify now | info@nado.org | Crystal Gateway Marriott; Hilton Arlington |
| BEBPA USB 2027 | BEBPA | College Park, MD | Med | College Park Marriott is home base but room block “coming soon”; FDA/USP audience supports Bethesda overflow / preferred lodging | Qualify now | contactus@bebpa.org | College Park Marriott; Hotel at UMD; Cambria College Park |
| Alexandria Soccer Kickoff 2027 | Alexandria Soccer Assn | Alexandria fields | Med | Stay-to-play via Traveling Teams — Maryland Beltway full-service can enter housing list | Qualify now | Traveling Teams / ASA site | Alexandria / Crystal City / National Harbor hotels |
| Arlington Spring Tournament 2027 | Arlington Soccer Assn | Arlington | Med | Hotel info forthcoming — early housing-list inclusion | Qualify now | tournaments@arlingtonsoccer.com | Arlington / Crystal City hotels |

### DMV Stretch

| Event | Org | Geography | Fit path | Bethesda Win Thesis |
|-------|-----|-----------|----------|---------------------|
| NADO WashCon 2027 overflow | NADO | Crystal Gateway (host locked) | Overflow only | Compete only if 2027 sells out like 2026 |
| Loudoun College Showcase 2027 | Loudoun Soccer | Loudoun / Dulles | Stretch housing | Only if HBC lists Maryland Beltway hotels |
| AAD Annual 2028 overflow | AAD | Washington, DC | Stretch overflow | Downtown medical congress; suburban overflow if blocks tighten |
| ECS 251st Meeting 2027 overflow | ECS | WEWCC + Marquis | Stretch overflow | Marquis is HQ — overflow only |
| ASAE Annual 2029 | ASAE | Washington, DC | Early watch | Long-lead overflow / satellite — too early for outbound |
| World Biomaterials Congress 2028 | WBC | WEWCC | Stretch overflow | Medical/research overflow if housing opens broadly |

Every Competitive/Stretch row includes a locked `demandTerritoryFit`, `bethesdaWinThesis`, and `marketCompetitors[]` (opportunity-market class) in addition to STR / Marriott-area comps.

---

## Rejected DMV opportunities (examples)

These were researched and **not** qualified as primary opportunities:

| Event | Why rejected |
|-------|----------------|
| CARH 2027 | Ritz-Carlton Pentagon City already contracted |
| ASOR 2027 | Hyatt Regency Crystal City contracted |
| IPWatchdog LIVE 2028 | Renaissance Arlington Capital View contracted |
| TEI Midyear 2027/28 | Grand Hyatt Washington block published |
| ASSA 2027 | Marriott Marquis headquarters named |
| NASPA 2027 | Gaylord National Harbor package |
| SPR Annual (DC years) | Hyatt Regency Capitol Hill contracted |
| Sacred Space 2027 | Hotel at UMD host locked |
| ONS Congress 2027 | Marquis + Westin headquarters locked |
| Data Center Americas 2027 | Marquis + Connections Housing locked |
| ACG Next 2027 | Single-day Tysons meeting — not a room block |
| NCCAN 2027 | Hyatt Regency Crystal City contracted |

This list is intentional proof of filtering, not scraping.

---

## Product / UI changes

- New module: `lib/group-demand-intelligence/dmv-expansion-pass.js`
- Apply script: `npm run gdi:apply-dmv-expansion`
- Demand Territory filter (All / Core / North DC / Competitive / Stretch)
- Territory summary KPI strip on internal + share UIs
- `bethesdaWinThesis` + `marketCompetitors` on opportunities / share sanitize
- Territory lock respected by enrichment classifier (`demandTerritoryFitLocked`)

---

## Success test checklist

| Criterion | Status |
|-----------|--------|
| New opportunities outside prior Bethesda-centric set | **PASS** (11 added) |
| Several credible DMV Competitive | **PASS** (5 qualified) |
| Some credible DMV Stretch | **PASS** (6 qualified) |
| Win Thesis on every non-core expansion row | **PASS** |
| Dynamic / opportunity-market competitors | **PASS** |
| No reduction in evidence quality / no fake volume | **PASS** (12 rejects documented) |
| Not “labels only” | **PASS** (research coverage changed) |

---

## Answer to the founder question

> Did the research universe actually expand beyond Montgomery County while preserving commercial credibility?

**Yes — partially.** Coverage now includes Arlington, Alexandria, College Park / PG, Loudoun, and downtown DC overflow watches with explicit win theses. Remaining gaps (deeper Fairfax/Tysons multi-day lodging RFPs; housing-bureau inventories) are better served by an optional **+$5–$10 Webhound** wave, not by inventing weak opportunities.
