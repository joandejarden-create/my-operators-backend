# Group Demand Intelligence — Bethesda Marriott Pilot Build Report

**Date:** 2026-09-12  
**Product:** Group Demand Intelligence (EXPERIMENTAL / PILOT)  
**Hotel:** Bethesda Marriott · Census `recLuxvwwxID7U2B8` · WASBT  
**Route:** `/group-demand-intelligence`  
**Latest run:** `gdi_run_mty24xps_4ed68381`

---

## Final verdict

# PARTIAL GO

An experienced Director of Sales would plausibly act on **several** of these opportunities this week (AMWA 2027, Bethesda Premier Cup housing, Potomac Memorial 2027 housing, NICE 2027 site inquiry). The prototype answers the commercial question for a **subset** of High/Medium leads with enough evidence to spend 10 minutes.

It is **not** yet a full GO for portfolio scale because:

- Corporate segment remains thin without CRM/Delphi
- Several medium leads are overflow-only (weaker than host-RFP leads)
- Ampfy is not configured (Apify reserved; Ampfy absent from repo)
- Live SerpAPI loop / shared event graph not yet productionized
- Qualified count (13) is healthy for a pilot, but High Priority depth is still thin (3)

---

## Architecture

### Reused

| System | Use |
|--------|-----|
| HI Research policy / $5 Webhound hard-cap doctrine | Cost discipline |
| Research-methods escalation ladder pattern | L1→L5 |
| Census hotelId as canonical key | `recLuxvwwxID7U2B8` |
| ADP Bethesda property profile | **READ-ONLY** seed for rooms/meeting space/comp set |
| Admin auth (`adminAuth`) | Run Research |
| Operator Fit feature-flag pattern | `GROUP_DEMAND_INTELLIGENCE_V1` |
| Market Demand standalone page pattern | Route isolation |
| Leak Audit isolation principle | No ADP writes |

### Extended

- New namespace: `lib/group-demand-intelligence/`, `api/group-demand-intelligence.js`, `data/group-demand-intelligence/`
- Explicit FACT / INFERENCE / ESTIMATED / UNKNOWN claim kinds
- Hotel Fit + Evidence Confidence + Priority + Booking Window scoring (config weights)
- GDI experimental research-method pack (not auto-promoted to HI production)
- Opportunity-linked contacts (not a parallel CRM database)
- Experimental Planner Consideration (labeled; does not touch ADP)

### New reusable research methods (EXPERIMENTAL)

`GDI-ASSOC-EVENT-01`, `GDI-MED-SCI-01`, `GDI-HIST-MEETING-01`, `GDI-GOV-EVENT-01`, `GDI-WEEKEND-01`, `GDI-CONTACT-01`, `GDI-ANCHOR-01`

### Build-rule compliance

- ADP scoring/methodology/published reports: **untouched**
- No parallel research engine
- No auto paid research on page load
- No destructive migrations
- Bethesda priorities in `config/group-demand-intelligence/hotels/recLuxvwwxID7U2B8.json`
- Gates: `npm run test:gdi-foundation` — **PASS**

### Explicitly NOT duplicated

Canonical hotel/company/person SoT, HI Webhound client stack (GDI records spend against same $5 doctrine), ADP observation history, Ampfy as SoT.

---

## Research architecture

| Level | What ran in Bethesda pilot |
|-------|----------------------------|
| L1 | ADP RO fixture + GDI demand config + census hotelId |
| L2 | GDI experimental methods applied to candidates |
| L3 | Official AMWA, NIH, NIST, MSYSA, tournament, SAM.gov, AFCEA pages |
| L4 | Ampfy `provider_not_configured`; Apify not invoked |
| L5 | Webhound session `4f99b00b-ca62-44af-b45c-3af61a6d325d` · **$5.00 / $5.00 hard cap** · 91 sources |

Escalation audited on each research run JSON under `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/runs/`.

---

## Bethesda results

| Metric | Count |
|--------|------:|
| Candidates researched | 22 |
| Qualified (salesperson view) | **13** |
| High Priority | **3** |
| Medium Priority | **6** |
| Watchlist | **4** |
| Disqualified (retained in audit) | **9** |
| Duplicates removed | 0 |
| Expired / closed removed from active | Included in disqualified (contracted venues, scale, timing) |

### High Priority

1. **AMWA 112th Annual Meeting 2027** — DC area; hotel not named; CONTACT NOW  
2. **Bethesda Premier Cup 2026** — stay-to-play housing via HBC; CONTACT NOW  
3. **Potomac Memorial Tournament 2027** — stay-to-play; CONTACT NOW for housing list

### Strong Medium (actionable)

4. **NICE Conference & Expo 2027** — location TBD (best open government RFP)  
5. **SHOW 2026 NIH** — campus overflow lodging  
6. **AFCEA Health IT Summit 2027** — competitor booked; overflow + 2028  
7. **MSYSA State Cup 2027** — multi-weekend sports  
8. **AMWA 2027 Interim** — TBD destination; DC pattern  
9. **NIH SBPO Vendor Outreach** — recurring pattern watch

---

## Source quality (directional)

| Class | Approx. share of important evidence rows |
|-------|------------------------------------------|
| Tier A first-party (org/gov/event official) | ~70%+ on High Priority |
| Tier B secondary | ~20% |
| Tier D inference / low-confidence | Present and labeled — not hidden |

No High Priority lead invents attendance or room blocks as FACT.

---

## Commercial review (5 leads)

### 1. AMWA 112th Annual Meeting 2027 — HIGH

| Question | Assessment |
|----------|------------|
| Real org/event? | Yes — official AMWA pages |
| Future opportunity credible? | Yes — dates + DC area; venue “coming soon” |
| Size plausible? | Likely mid-size (“hundreds” in 2026 recap) — peak rooms still UNKNOWN |
| Bethesda fit? | Sensible for medical + Advocacy Day logistics |
| Timing useful? | Yes — CONTACT NOW |
| Contact relevant? | Public meetings inbox — usable start |
| Action sensible? | Yes |
| Would DOS/GM act? | **Yes** |
| Evidence sufficient? | **Yes for a first call** |

**Weakness:** Exact peak rooms unknown; downtown DC hotels still compete.

### 2. Bethesda Premier Cup 2026 — HIGH

| Question | Assessment |
|----------|------------|
| Real? | Yes — sanctioned MSYSA tournament |
| Credible? | Yes — stay-to-play + HBC housing |
| Size? | UNKNOWN rooms; commercially real weekend demand |
| Fit? | Strong local weekend fit |
| Timing? | Yes |
| Contact? | Tournament director + housing company named |
| DOS act? | **Yes** |

**Weakness:** Hotel may already be on/off the list; conversion depends on HBC relationships.

### 3. Potomac Memorial 2027 — HIGH

| Question | Assessment |
|----------|------------|
| Real? | Yes — recurring ~450-team tournament |
| Credible? | Yes — stay-to-play policy |
| Timing? | 2027 dates inferred from pattern (labeled) |
| DOS act? | **Yes** — housing-list inclusion ask |

**Weakness:** Exact 2027 dates not posted yet; competitors already on 2026 Roomvy list.

### 4. NICE 2027 — MEDIUM (commercially strong)

| Question | Assessment |
|----------|------------|
| Real? | Yes — NIST official page |
| Venue open? | Yes — TBA |
| Fit? | Good if Maryland shortlisted; geography still open |
| DOS act? | **Yes** — site inquiry worth 10 minutes |

**Weakness:** Hotel Fit 73 just below High threshold — honest scoring; destination may leave Maryland.

### 5. AFCEA Health IT Summit 2027 — MEDIUM

| Question | Assessment |
|----------|------------|
| Real? | Yes |
| Host available? | **No** — Bethesda North Marriott booked |
| Still useful? | Overflow + 2028 cycle |
| DOS act? | **Maybe** — lower urgency than open RFPs |

**Weakness:** Not a primary win for 2027; easy to over-credit competitor-loss leads.

---

## Research cost

| Item | USD |
|------|----:|
| Total run cost | **5.00** |
| Webhound | **5.00** |
| Ampfy | 0.00 (not configured) |
| Apify | 0.00 |
| SerpAPI (this run) | 0.00 |
| Per qualified opportunity | ~0.38 |
| Per High Priority | ~1.67 |

Webhound hard cap honored (no silent overrun).

---

## Webhound ROI

| Metric | Value |
|--------|-------|
| Calls | 1 report session |
| Cost | $5.00 |
| Sources | 91 |
| Question | Future group demand opportunities for Bethesda Marriott (specific meetings) |
| Material improvements | Added NICE TBA venue, Potomac Memorial housing path, SHOW 2026, AFCEA competitor intelligence, Georgetown/MSYSA weekend paths; confirmed AMWA/Premier Cup |

**ROI judgment:** Worth the $5 for this hotel. Prefer spending deeply on a handful of High/Medium candidates again at scale — not spraying across dozens.

Artifact: `data/group-demand-intelligence/research-artifacts/webhound-bethesda-4f99b00b-output.json`

---

## Ampfy value

| Metric | Value |
|--------|-------|
| Calls | 0 |
| Status | `provider_not_configured` |
| Note | Repo has **Apify**, not Ampfy. Adapter reserved. Did not become SoT. |

---

## Performance

| Area | Notes |
|------|-------|
| Research run | Seconds for L1–L3 seed merge; Webhound ~75 minutes wall-clock |
| Page load | Static HTML + JSON APIs; no paid research on GET |
| Caching | Persisted run artifacts under `data/group-demand-intelligence/` |
| Expensive ops | Webhound only on admin Run Research / CLI with explicit session |

---

## Missing data (material accuracy upgrades — not required for V1)

- Delphi / FDC / CI/TY historical groups  
- CRM production / lost business  
- Cvent RFP feed  
- Need dates / pace / occupancy / ADR  
- Negotiated account lists  
- Meeting space capacity detail beyond ADP fixture  

---

## Learning opportunities → native methods

| Pattern learned | Proposed native method evolution |
|-----------------|----------------------------------|
| “Dates announced + venue TBA” on association pages | Strengthen `GDI-ASSOC-EVENT-01` venue-status detector |
| Stay-to-play housing companies (HBC, Roomvy) | New `GDI-WEEKEND-HOUSING-01` |
| Competitor-booked local summits (AFCEA @ North Marriott) | `GDI-COMPETITOR-VENUE-DETECT-01` |
| NIST/SAM.gov public event TBA | Expand `GDI-GOV-EVENT-01` |
| NIH campus events → overflow not host | Explicit demand-type `campus_overflow` |

Do **not** auto-promote to PRODUCTION_READY without regression packs.

---

## ADP integration assessment (do not integrate yet)

1. **Could become ADP capability later?** Optional “action layer” for Meetings & Groups territory — not a scoring input.  
2. **Should remain separate for now?** **Yes.** Different unit of analysis (specific demand piece vs AI presence).  
3. **Action layer above ADP?** Promising long-term composition.  
4. **Experimental Planner Consideration useful?** Mildly — helps ask “are we in the consideration set?” without polluting Presence Index.  
5. **Methodology confusion risk if combined?** **High** if scores/observations mix. Keep contracts separate.  
6. **Shared infrastructure:** hotelId, evidence patterns, research escalation, spend guards, census identity.

---

## Scale assessment

| Scale | Requirement |
|-------|-------------|
| 10 hotels | Viable with $5 Webhound selective + shared org/event cache |
| 100 hotels | **Must** research events once, evaluate many hotels; otherwise cost explodes |
| 1,000 hotels | Requires shared event intelligence graph + heavy caching + mostly L1–L3; Webhound only for edge cases |

Design note already encoded: opportunities reference organizations/events independently of a single hotel so multi-hotel evaluation can reuse research.

---

## How to run locally

```bash
# .env
GROUP_DEMAND_INTELLIGENCE_V1=1
GROUP_DEMAND_INTELLIGENCE_PILOT_READ=1

npm run gdi:bethesda-pilot-run -- --webhound-session=<optional> --webhound-cost=5
npm run test:gdi-foundation

# UI (signed-in Dealality user; Run Research requires admin)
# http://localhost:<port>/group-demand-intelligence
```

---

## Quality gates checklist (Phase 21)

1–15: Addressed for High Priority set (evidence, why hotel, why now, no invented blocks/attendance/contacts as FACT, duplicates none, expired/contracted disqualified, FACT/INFERENCE separated, scores reconstructable, escalation audited, costs visible).  
16: Webhound ≤ $5 — **PASS**  
17: Ampfy not SoT — **PASS**  
18–19: No parallel engine / contact DB — **PASS**  
20: ADP unchanged — **PASS**  
21–22: Build rules + config-not-hardcode — **PASS**  
23–25: Page does not auto-run paid research; admin gate on Run — **PASS**

---

## Product answer

> Would an experienced hotel salesperson actually use these opportunities, pursue them and potentially generate RFPs or revenue?

**For AMWA 2027, Premier Cup housing, Potomac Memorial housing, and NICE TBA venue: yes — pursue.**  
**For the full list as a product: partially — needs CRM-backed corporate demand and shared event graph before portfolio GO.**

**Verdict: PARTIAL GO.**
