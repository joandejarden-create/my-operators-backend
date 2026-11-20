# Bethesda Phase 3 — Sources / Evidence Completeness Gate

**Generated:** 2026-11-19T15:14:20.438Z
**Apply:** BLOCKED (dry-run only)

## Sources tab audit (established path)

| Layer | Implementation |
|---|---|
| UI tab | `Sources & Evidence` in `hotel-intelligence-workspace.js` |
| Citation builder | `buildCitations()` ← `ownership.report.evidence.sources` |
| API | `GET` golden-demo ownership (`api/golden-demo-ownership.js`) |
| Airtable SoT | **HI Graph Evidence** on HPC base `appCCUsuGsE1ifoLk` |
| Reader | `listEvidenceForHotel` → `projectEvidenceToSourcesCitations` |
| Not used for HE ownership evidence | Partner Intelligence Sources API (separate product) |

## Dry-run operation counts

| Table | CREATE | UPDATE |
|---|---:|---:|
| HI Organizations | 3 | 0 |
| HI Hotel–Org Relationships | 2 | 0 |
| HI Org Contacts | 3 | 0 |
| HI Contact Channels | 2 | 0 |
| HI Graph Evidence | 11 | 0 |
| HI Ownership Research Runs | 1 | 0 |

**Evidence CREATE:** 11 (was 0 in prior dry-run — corrected)

## Canonical evidence documents (deduped by URL)

- **Maryland SDAT Real Property — Montgomery County acct 03547343** — https://sdat.dat.maryland.gov/RealProperty/Pages/default.aspx
  - Supports: property_company / Legal Owner: Marbeth Acquisition LLC
  - Current: true; Limitation: Does not independently prove ultimate beneficial ownership unless explicitly stated.
- **Mullen Real Estate Capital — Portfolio** — https://mullencap.com/portfolio/
  - Supports: economic_owner: Mullen Real Estate Capital
  - Current: true; Limitation: Sponsor association from owner website portfolio. Distinct from SDAT legal title. Does NOT prove natural-person UBO.
- **Mullen Real Estate Capital — Contact** — https://mullencap.com/contact/
  - Supports: emails_phones / org channels: info@mullencap.com; 7 Campus Blvd Suite 300 Newtown Square PA 19073
  - Current: true; Limitation: Does not independently prove ultimate beneficial ownership unless explicitly stated.
- **Mullen Real Estate Capital — About / Team** — https://mullencap.com/about/
  - Supports: decision_authority.people / key_people: Jeff Mullen; Katherine Button; Patrick Reece
  - Current: true; Limitation: Supports named executive roles at MREC. Does not establish qualified signatory authority or UBO.
- **Mullen Hospitality Management — Portfolio** — https://mullenhospitality.com/portfolio/
  - Supports: asset_agreement_profile: Ops & Investment Supervision; brand-managed (Marriott)
  - Current: true; Limitation: Does not independently prove ultimate beneficial ownership unless explicitly stated.
- **Hodges Ward Elliott — Bethesda Marriott sale (Dec 2018)** — https://hodgeswardelliott.com/news-posts/hodges-ward-elliott-represents-owner-in-the-sale-of-the-407-key-bethesda-marriott-in-bethesda-maryland/
  - Supports: property_history / asset_agreement_profile: Sale announced; continues managed by Marriott International
  - Current: false; Limitation: Historical / contextual only — does not by itself establish current ownership.
- **Hotel Online — Northview/Oaktree acquire Bethesda Marriott (2013)** — https://www.hotel-online.com/news/northview-hotel-group-affiliate-of-oaktree-capital-management-acquire-the-b
  - Supports: property_history: Northview Hotel Group + Oaktree affiliate acquisition
  - Current: false; Limitation: Historical / contextual only — does not by itself establish current ownership.
- **Ramsfield Hospitality Finance — Bethesda Marriott loan** — https://www.ramsfieldrealestate.com/projects/bethesda-marriott
  - Supports: property_history: $63,000,000 stretch senior acquisition financing
  - Current: false; Limitation: Historical / contextual only — does not by itself establish current ownership.
- **Washington Business Journal — related headline (Mullen family sale)** — https://www.bizjournals.com/washington/news/2019/06/11/bethesda-north-marriott-completes-25m-renovation.html
  - Supports: economic_owner corroboration: Related article headline: Bethesda Marriott sold to Mullen family
  - Current: true; Limitation: LIMITATION: full article body not retrieved. Corroborative only — SDAT + MREC portfolio remain primary. UBO still UNRESOLVED.
- **SAM / OpenGovUS — Marriott Hotel Services LLC DBA Bethesda Marriott** — https://opengovus.com/sam-entity/M8CAG8UV6174
  - Supports: operator_not_owner: Marriott Hotel Services, LLC (operator)
  - Current: true; Limitation: Does not independently prove ultimate beneficial ownership unless explicitly stated.
- **Trivago — Bethesda Marriott 4-star listing** — https://www.trivago.com/en-US/oar/hotel-bethesda-marriott?search=100-214526
  - Supports: starRating: 4 Stars
  - Current: true; Limitation: Does not independently prove ultimate beneficial ownership unless explicitly stated.

## Claim distinctions preserved

- SDAT → Legal Owner / PropCo (current title)
- MREC portfolio + contact mailing → Economic Owner *sponsor association* (not UBO)
- MREC About → named executives (not qualified signatories)
- HWE / Hotel Online / Ramsfield → history / agreement context (`current=false` where historical)
- UBO remains **UNRESOLVED**

## Completeness gate

| Check | Status |
|---|---|
| All Phase 3 sources in evidence write-back | PASS (11 docs) |
| Every new ownership/org/contact claim has evidence | PASS (mapped in staged package) |
| Sources tab can retrieve via established API shape | PASS (preview citations) |
| No evidence-only-in-local-JSON after apply | GATED (requires founder apply) |
| Idempotent replay | PASS (2nd pass CREATE=0) |
| No parallel evidence store | PASS |

**STOP before production Airtable apply.**

