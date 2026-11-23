# GDI Portfolio Certification V1.3 — Founder Report

Version: `gdi-portfolio-cert-v1-3-evidence-integrity-actionability-v1`
Generated: 2026-11-23T11:35:19.918Z
Mode: **$0 validation + controlled remediation** (no paid Exa, no Airtable, no publication).

## 1. PR #564 / #566 integration status

- {"pr564":"OPEN_AT_LAST_CHECK","pr566":"OPEN_AT_LAST_CHECK","note":"GDI product chat does not merge — Integration Controller owns #564 then #566 before or with V1.3 sequencing."}
- Both PRs CI-green / MERGEABLE at last check; **not yet merged into main**.
- GDI product chat does not merge. IC must merge **#564 then #566** (dependency order) before claiming main-integrated V1.2.
- V1.3 branch is cut from the #566 tip to avoid duplicate V1.1 commits.

## 2. Evidence propagation test results

- Fixture pass: **true**
- Mapped investigatedUrls include housing URL: https://example.org/hotel-housing, https://example.org/event-announcement
- Empty-provider completion complete=false: **true** (INCOMPLETE_EMPTY_INVESTIGATED_URLS)
- Genuine EXCLUDE negatives pass: **true**

## 3. Actual investigated URL traceability (frozen V1.1 eight)

| Opportunity | Seed | Investigated | Completion | Improvement |
|---|---:|---:|---|---|
| disc_461dfa26a790 (American Society for Gravitational and Space Research) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_83f5b13dff4c (Liatris, Inc.) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_0a11a0bb1158 (American Association of Exporters and Importers) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_9334f4f1e3dd (American Hospital Association) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_cd8f10005345 (International Road Federation) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_b7fc48bf6995 (European Society of Coloproctology) | 2 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_a1a0378bf466 (Society of Environmental Toxicology and Chemistry (SETAC Europe)) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |
| disc_77bac9a0d82a (Agroscope / GDPM 2027 organizers) | 1 | 0 | INCOMPLETE_EMPTY_INVESTIGATED_URLS | PAID_CALL_WITHOUT_EVIDENTIARY_IMPROVEMENT |

Frozen investigatedUrls still empty on **8/8** (V1.1 defect). V1.2/V1.3 propagation fixes apply to future runs; frozen ledger is not rewritten as if URLs existed.

## 4–7. Three-candidate commercial review (AAEI / AHA / ESCP)

### American Association of Exporters and Importers (disc_0a11a0bb1158)
- Event: AAEI 106th Annual Conference & Trade Day 2027 | 2026-11-23 research: event 2027-06-22 to 2027-06-24 (future — timing valid)
- Venue: Ronald Reagan Building & International Trade Center, Washington, DC
- Organizer: AAEI (association-owned event)
- Named lodging buyer: none verified
- Hotel sourcing evidence: Official room block NOW OPEN at Hotel Washington (515 15th St NW, Washington, DC) — competitor/host lodging committed.
- Public contact: ORGANIZATION_ROUTE events@aaei.hq.org
- Pilot hotel relevance: LOW for Bethesda Marriott as primary — official block is downtown DC Hotel Washington; Bethesda is not the published housing hotel. Overflow not evidenced.
- Lodging demand validated (any hotel): true; for pilot hotel: false
- Disposition: **STOP_PRIMARY_COMPETITOR_COMMITTED** | Sales Ready: false
- Continue: false — Primary lodging decision closed at competitor. No overflow evidence. Do not spend paid research for Bethesda primary pursuit.

### American Hospital Association (disc_9334f4f1e3dd)
- Event: AHA Annual Membership Meeting 2027 | 2026-11-23 research: event 2027-04-18 to 2027-04-20 (future — timing valid)
- Venue: Marriott Marquis Washington, D.C.
- Organizer: American Hospital Association
- Named lodging buyer: none verified
- Hotel sourcing evidence: AHA is official housing provider; negotiated block at Marriott Marquis Washington, D.C. ($371/night) expires 2027-03-17.
- Public contact: ORGANIZATION_ROUTE annualmeeting@aha.org
- Pilot hotel relevance: LOW for Bethesda Marriott as primary — host hotel is Marriott Marquis DC. Bethesda could only be overflow/displacement; overflow not evidenced on public pages.
- Lodging demand validated (any hotel): true; for pilot hotel: false
- Disposition: **STOP_PRIMARY_COMPETITOR_COMMITTED** | Sales Ready: false
- Continue: false — Confirmed competing host hotel + official housing. Paid deep research for Bethesda primary is not commercially justified without overflow evidence.

### European Society of Coloproctology (disc_b7fc48bf6995)
- Event: ESCP 2027 – 22nd Scientific Meeting | 2026-11-23 research: event 2027-09-22 to 2027-09-24 (future — timing valid)
- Venue: Palexpo, Route François-Peyrot 30, 1218 Le Grand-Saconnex, Geneva
- Organizer: ESCP Secretariat (c/o Lindsays, Edinburgh)
- Named lodging buyer: none verified
- Hotel sourcing evidence: Venue confirmed at Palexpo; public site states 'More information will follow soon' — no official room block published yet.
- Public contact: ORGANIZATION_ROUTE ESCP Secretariat c/o Lindsays, Caledonian Exchange, 19a Canning Street, Edinburgh EH3 8HE, United Kingdom
- Pilot hotel relevance: WEAK — YOTEL Geneva Lake (Founex) is ~9 miles / ~15 min from Palexpo; closer properties sit at Palexpo (Hilton / ibis). Not a natural primary congress hotel.
- Lodging demand validated (any hotel): false; for pilot hotel: false
- Disposition: **WATCHLIST_LOW_PRIORITY_OR_STOP** | Sales Ready: false
- Continue: true — Free public pass only: monitor housing partner announcement; do not pay until a named housing contact or open RFP appears AND YOTEL Lake relevance is defended.

## 8. Research-continuation quality

- **#1 Liatris, Inc.**: CONTINUE_PUBLIC_THEN_TOP_PAID_CANDIDATE — HIGHEST of eight — named executives + Rockville proximity
- **#2 ASGSR**: CONTINUE_FREE_THEN_CONDITIONAL_PAID — MODERATE — multi-day meeting; lodging page may exist
- **#3 European Society of Coloproctology**: CONTINUE_FREE_ONLY — LOW_TO_MODERATE (free monitor only)
- **#4 International Road Federation**: CONTINUE_FREE — MODERATE — confirm destination + housing
- **#5 SETAC Europe**: CONTINUE_FREE — MODERATE — Palexpo-class scientific meeting
- **#7 GDPM / Agroscope**: STOP_OR_ARCHIVE_LOW — LOW — niche workshop; hotel fit unclear
- **#8 American Association of Exporters and Importers**: STOP_PRIMARY — NEAR_ZERO for pilot-hotel primary
- **#8 American Hospital Association**: STOP_PRIMARY — NEAR_ZERO for pilot-hotel primary

## 9. Before/after commercial qualification

- V1.2 verification pass: **true** (WATCHLIST=8, EXCLUDE=0, Sales Ready=0, publication=0)
- WATCHLIST does **not** imply verified pilot-hotel demand (eligibility gaps retained).
- Public re-review: 2 of priority-three are STOP_PRIMARY_COMPETITOR_COMMITTED; 0 pilot-hotel lodging demand validated.

## 10. Recommended next paid pilot (design only)

- Aggregate max: **$1.5** across ≤3 opps (Liatris, ASGSR, IRF).
- Excluded from paid: AAEI, AHA (competitor housing committed), ESCP (weak YOTEL↔Palexpo fit — free monitor only).
- **Requires separate founder GO** — not executed in V1.3.

## 11. CI results

- See PR checks after open; local suites in test plan.

## 12. Readiness verdict

**LIMITED_PILOT_CONTINUATION_DESIGN_READY**

System can preserve incomplete research without false EXCLUDE and can distinguish competitor-committed lodging from open pilot-hotel demand. Not certified for broader hotel scaling.

## STOP

Return for founder review before additional paid research, live persistence, customer publication, or broader rollout.

`AWAITING_INTEGRATION_CONTROLLER`