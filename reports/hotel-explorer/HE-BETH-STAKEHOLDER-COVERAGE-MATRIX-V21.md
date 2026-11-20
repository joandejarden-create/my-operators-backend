# Bethesda Marriott — Stakeholder Research Coverage Matrix (V21)

**Hotel:** Bethesda Marriott · HPC `recLuxvwwxID7U2B8`  
**Date:** 2026-11-19 · **PR:** #463 · **Lesson:** HE-QL-033  
**Rule:** A category is not “researched” merely because the graph supports that node type.  
**Persistence:** Phase 3/4 Airtable apply remains **blocked** (`applyAuthorized: false`).

Two independent quality measures:

| Measure | Meaning |
|---|---|
| **Stakeholder Research Coverage** | Which categories were investigated and what was verified |
| **Network Visualization Coverage** | Which verified (or honestly historical) relationships render correctly |

---

## Coverage matrix

| Category | Verified (research) | Staged / proposed | Elsewhere, not connected | Not performed | Done — no reliable result | Canonical Airtable | Status |
|---|---|---|---|---|---|---|---|
| **Legal ownership** | Marbeth Acquisition LLC — MD SDAT Hotel Unit; 2019-01-09 deed $69.5M from Bethesda Hotel Acquisition LLC | PropCo + `Legal Owner` HI Rel dry-run | Pre-2019 Northview/Oaktree package (stale for current) | Business-entity member/manager filings beyond SDAT | — | HI Organizations + HI Hotel–Org Rel + Evidence | **RESEARCHED · VERIFIED** (persist blocked) |
| **Economic ownership** | Mullen Real Estate Capital sponsor (portfolio + HQ mailing match) | `Economic Owner` Rel MEDIUM | Older tabs-1–5 Northview file superseded | LP / PE stack | **UBO natural person UNRESOLVED** | HI Orgs + Rel + Evidence (do not overwrite HPC Parent Company) | **RESEARCHED · SPONSOR VERIFIED · UBO GAP** |
| **Asset management** | Mullen Hospitality Management = ops/investment supervision posture; Button & Reece titles at MREC | MHM org staged; people INFERRED/UNKNOWN property link | MHM team advisors vs hotel assignment | Property-assigned AM confirmation | Named Bethesda AM assignment | HI Orgs (Asset Manager) + Org Contacts | **FIRM RESEARCHED · PROPERTY ASSIGNMENT GAP** |
| **Financing** | Historical only: Ramsfield Hospitality Finance ~$63M stretch senior acquisition financing (case study) | Evidence as property_history; OCN historical `FINANCED_BY` when Capital group open | OCN type supported; no current edge | Current mortgagee / refinance post-2019 | **Current lender identity** | Evidence (historical); Rel only if current validated | **HISTORICAL VERIFIED · CURRENT GAP** |
| **Equity / JV** | None for current structure | — | Generic JV vocabulary elsewhere | Controlling investors / JV / preferred equity | — | HI Orgs + Rel when found | **NOT RESEARCHED TO RESULT** (pass complete: no public current JV found) |
| **Operator** | Marriott International / managed — live HI edges; HWE 2018 “continues managed by Marriott” | Not rewritten by Phase 3 | SAM ops contacts deliberately not ownership | Mgmt agreement commercial terms | — | Existing Operator Rel | **LIVE VERIFIED** |
| **Brand / franchisor** | Marriott Hotels brand live | Agreement posture derived; no invented franchise text | Brand must never fill Economic Owner | FDD / franchise terms | Franchise vs managed classification beyond posture | Brand Rel + Evidence | **LIVE VERIFIED · FRANCHISE NOT ASSERTED** |
| **Development** | Open operating hotel — material owner-side development **NOT_APPLICABLE**; Exeter tower at 5151 Pooks Hill excluded (different parcel) | Run note only | Adjacent residential press excluded | CapEx beyond public headlines | Material development pursuit: none | Research Runs note | **RESEARCHED · N/A** |
| **Advisors** | Hodges Ward Elliott — Dec 2018 sale broker (seller) + capital markets for purchaser financing | Historical evidence; OCN historical `ADVISED_BY` in Development group | — | Current FA / broker / counsel | Current advisors | Evidence historical; Rel if current | **HISTORICAL VERIFIED · CURRENT GAP** |
| **Key people** | Jeff Mullen (CEO), Katherine Button (VP AM), Patrick Reece (Director AM) — LinkedIn + org roles | Contact staging + Surfe session CTA | SAM operator contacts | Paid Surfe | Personal email/phone; Bethesda-assigned AM | HI Org Contacts + Channels | **IDENTITY VERIFIED · CHANNELS / ASSIGNMENT GAP** |
| **Related hotels** | Bethesda matched; MREC website portfolio list | 7 other current candidates HPC-unresolved; realized/historical excluded from current | — | Full ownership verification per asset | “Full portfolio” claim | OWNED/CONTROLLED only for HPC-matched | **PARTIAL · PROPOSED PORTFOLIO** |

---

## Free-first research findings (this pass)

### A. Asset management
- **Firm:** Mullen Hospitality Management publicly presents portfolio ops / investment supervision including Bethesda-class assets ([mullenhospitality.com](https://mullenhospitality.com/portfolio)).
- **People:** Button / Reece hold AM titles at MREC; **not** auto-assigned as Bethesda property AMs.
- **Separate AM company:** MHM is affiliate/ops brand under Mullen family hospitality — staged as Asset Manager org, not a third-party unrelated AM firm.

### B. Financing (Ramsfield)
- [Ramsfield case study](https://www.ramsfieldrealestate.com/projects/bethesda-marriott): **~$63,000,000** stretch senior first mortgage for **acquisition** of 407-room Bethesda Marriott; RHF held prior mortgage; buyer unnamed on lender page.
- Related index note (~Aug 2018) cites **$63,750,000** refinance-style case study — treat as **historical lender marketing**, not proof of today’s mortgagee.
- **Classification:** `HISTORICAL` · **Current lender:** unresolved.

### C. Equity / JV
- No public current JV / co-invest party identified for Marbeth / Bethesda beyond MREC sponsor attribution.
- Do not infer ownership from shared addresses or operator relationships.

### D. Developers & advisors
- **Broker (historical):** [Hodges Ward Elliott Dec 3, 2018](https://hodgeswardelliott.com/news-posts/hodges-ward-elliott-represents-owner-in-the-sale-of-the-407-key-bethesda-marriott-in-bethesda-maryland/) — represented former owner; capital markets also assisted purchaser financing; Marriott continues as manager.
- **Original developer:** not established in this free pass (operating hotel; construction-era developer not required for current decision-maker map).
- **Current advisors:** unresolved.

### E. Brand-development relevance (rationale only — no imminent deal asserted)
| Stakeholder | Useful for |
|---|---|
| MREC / Button / Mullen | Ownership investment, renovation/repositioning discussions, affiliation conversations with economic sponsor |
| MHM | Owner-side asset/ops coordination |
| Marriott International / Marriott Hotels | Management agreement and brand standards |
| HWE (historical) | Transaction process context only — not a current decision-maker |
| Ramsfield (historical) | Capital structure history only — not current lender outreach target |

---

## Newly staged for visualization / dry-run (not Airtable-applied)

1. MHM as **Asset Manager** (proposed) on default network when supported.  
2. Ramsfield as **Historical financing** — Capital & Financing group only.  
3. HWE as **Historical advisor** — Development & Advisors group only.  
4. Related Hotels as **cluster node** (not unbounded canvas dump).  
5. Relationship-group selectors + research-status messages for empty categories.

Dry-run: `reports/hotel-explorer/phase4-quality/HE-BETH-STAKEHOLDER-UPSERTS-DRY-RUN-V1.json`

## Remaining gaps (refresh-eligible)
1. Current lender / mortgagee  
2. Property-assigned asset manager  
3. UBO / controlling shareholding  
4. Current advisors  
5. Equity/JV if any private structure surfaces  
6. HPC match for 7 Mullen portfolio candidates  
7. Personal contact channels (Surfe gated, session-only)  
8. Authorize Phase 3 HI graph apply  

## STOP
No production Airtable apply · no paid research · no deploy.
