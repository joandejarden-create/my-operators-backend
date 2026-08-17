# Operator Explorer — Brand-Managed Operator Universe Discovery

> **Classification superseded (2026-08-10):** Use `reports/operator-explorer-brand-managed-universe-normalized.md` for Operating Model × Management Availability. This file remains the discovery trail (parents, seed, Core 5).

**Date:** 2026-08-09  
**Mode:** Read-only discovery + architecture design  
**No Airtable fields/tables created**  
**Sources:** Live Brand Basics (258 brands / 34 parents), Active/Live subset (62 / 10 parents), existing Core 5 Managed Masters, seed list §C, Fit `CANDIDATE_TYPE` / Shortlist Candidate Type

**Machine dumps:**  
- `reports/operator-explorer-brand-managed-universe-discovery.json`  
- `reports/operator-explorer-brand-basics-all-parents.json`

---

## A. Candidate Type — reuse vs add

### Existing taxonomies

| Location | Options today | Usable? |
| -------- | ------------- | ------- |
| Fit config `CANDIDATE_TYPE` | `Third-Party Operator`, `Brand Managed` | Too narrow |
| Shortlist.`Candidate Type` | `Third-party operator`, `Brand-managed`, `Hybrid`, `Research Stage`, `Active / production` | **Partially** — last two are **lifecycle**, not candidate types (pollution) |
| Master | No Candidate Type field | — |
| Company Profile.`Operating Model` / role | Own-and-Operate, Third-Party Management, Mixed, … | Platform onboarding; related but not Explorer Candidate Type |

### Recommended controlled vocabulary (dry-run design only — **do not create field yet**)

| Value | Meaning |
| ----- | ------- |
| Third-Party Operator | Independent manager (Highgate, Arbor, GHL, …) |
| Brand-Managed Operator | Brand company / brand management division offering management to owners (MxM, Hilton Managed, …) |
| Integrated Brand / Operator | Brand company that primarily owns+operates (or tightly integrated owner-operator brand) |
| Owner-Operator | Owner platform that operates its own assets; may selectively 3P |
| Asset Manager | Asset-management-led; ops via others or selective |
| Hybrid | Material mix of the above — must document which modes |
| To Be Confirmed | Classification incomplete |

**Recommendation:** Reuse Shortlist Candidate Type as the **workflow** enum after cleanup; plan Master.`Candidate Type` (or Profile field) with the cleaned vocabulary for Explorer.  
**Do not** keep `Research Stage` / `Active / production` inside Candidate Type — those stay on `submission_status`.

---

## B. Brand Explorer parent discovery

| Universe | Brands | Distinct Parent Company values |
| -------- | -----: | -----------------------------: |
| Brand Basics (all statuses) | 258 | **34** |
| Brand Status Active/Live | 62 | **10** |

Active/Live parents only are **not** the full Brand Operator candidate universe — many luxury/integrated parents sit in Draft/Under Review Brand Basics rows.

### Management-capability classes

| Class | Definition |
| ----- | ---------- |
| Confirmed Direct Manager | Official managed-hotel / development program evidenced; offers (or routinely provides) management to third-party owners for in-scope brands |
| Direct Manager — Limited Brands / Markets | Management offered, but scoped to subsets of brands, scales, or geographies |
| Integrated Brand / Operator | Primarily owns/operates branded portfolio; management-to-third-parties limited/selective/unclear |
| Franchise / Brand Only | Franchise/license/affiliation primary; direct management not a material owner pathway |
| Management Availability Unknown | Plausible but not yet evidenced for Dealality |
| Not Applicable | Soft brand / rep collection / placeholder / non-operator |

---

## C–D. Classification (Brand Basics parents + seed)

**Evidence posture:** Classifications below use (1) existing Dealality source packs / Core 5 docs, (2) well-known official management models, (3) **Unknown** where this phase did not complete fresh official-source verification. Deep dry-run research is Track 2.

### Parents present in Brand Basics (evaluated)

| Parent (Brand Basics) | Active/Live brands | Class | Existing OE Master | Notes / scope |
| --------------------- | -----------------: | ----- | ------------------ | ------------- |
| Marriott International, Inc. | 15 | **Confirmed Direct Manager** | `Marriott International (Managed)` `recGmiPhRt6hiayd9` | MxM program; brand/geo scoped — not all flags equally managed |
| Hilton Worldwide | 13 | **Confirmed Direct Manager** | `Hilton (Managed)` `rec3Uwxe6ovpiokuN` | Hilton Management Services / managed path; brand-scoped |
| AccorHotels | 9 | **Confirmed Direct Manager** | `Accor (Managed)` `recF2WqLqNVyKGz9E` | Managed + franchise mix; brand/region scoped |
| InterContinental Hotels Group | 7 | **Direct Manager — Limited** | `IHG Hotels & Resorts (Managed)` `rec7IXYQYpKMYsrDl` | Franchise-heavy; management concentrated on certain brands/markets |
| Hyatt Hotels Corporation | 1 | **Confirmed Direct Manager** | *none* | Strong managed presence especially full-service/luxury; franchise growing — **scope required** |
| Minor Hotel Group Limited | 0 Active | **Confirmed Direct Manager** / Integrated traits | `Minor Hotels (Managed)` `rec8SrT3VjRkkYTxm` | Includes NH etc.; do **not** create separate NH Master |
| Sonesta International Hotels Corporation | 0 (all Draft) | **Confirmed Direct Manager** | *none* | Historically management-heavy US platform |
| Radisson Hotel Group | 0 (Draft) | **Direct Manager — Limited** | *none* | Management + franchise/license mix |
| Wyndham Hotels & Resorts | 2 | **Franchise / Brand Only** | *none* | Franchise-primary; management not default owner pathway |
| Choice Hotels International | 11 | **Franchise / Brand Only** | *none* | Franchise-primary |
| BWH Hotels | 2 | **Franchise / Brand Only** | *none* | Franchise/membership |
| Four Seasons Hotels and Resorts | 0 (Draft) | **Confirmed Direct Manager** | *none* | Primarily managed model |
| Mandarin Oriental Hotel Group | 0 | **Integrated Brand / Operator** | *none* | Owned + managed; selective |
| Rosewood Hotel Group | 0 | **Confirmed Direct Manager** | *none* | Management-led luxury |
| Aman Group | 0 | **Integrated Brand / Operator** | *none* | Owned/managed collection |
| Shangri-La Hotels and Resorts | 0 | **Integrated Brand / Operator** | *none* | Owned + managed |
| The Peninsula Hotels | 0 | **Integrated Brand / Operator** | *none* | HSH; owned/managed |
| Oetker Hotels | 0 | **Integrated Brand / Operator** | *none* | Collection owned/managed |
| Iberostar Hotels & Resorts | 0 | **Integrated Brand / Operator** | `Grupo Iberostar` `recwEHUotSGpfkZEJ` (not `(Managed)` naming) | Owner-operator resort; use **one** Master — not a second “Iberostar Managed” |
| Preferred Hotels & Resorts | 1 | **Not Applicable** | — | Soft brand / membership |
| Small Luxury Hotels of the World | 1 | **Not Applicable** | — | Affiliation network |
| Leading Hotels of the World | 0 | **Not Applicable** | — | Representation/marketing |
| Banyan Tree Hotels & Resorts | 0 | **Direct Manager — Limited** | *none* | Luxury resort; selective management |
| Hyatt Vacation Ownership | 0 | **Not Applicable** | — | Timeshare — not hotel BM pathway |
| Red Roof Franchise, UK | 0 | **Franchise / Brand Only** | — | |
| Staycity Ltd | 0 | **Management Availability Unknown** | — | Extended-stay operator; confirm BM vs own |
| Dovetail + Co / Prem Group / Edyn / Coast / AmeriVu / Northland / Dealality placeholder / (no parent) | — | **Unknown** or **N/A** | — | Not Brand Operator candidates for OE Track 2 |

### Seed companies **not** in Brand Basics Parent Company today

| Seed company | Class (pending Brand Basics onboarding) | Action |
| ------------ | --------------------------------------- | ------ |
| Belmond | Integrated Brand / Operator | Discover-only until Brand Basics parent exists |
| Kerzner International | Integrated Brand / Operator | Same |
| Dorchester Collection | Integrated Brand / Operator | Same |
| Langham Hospitality Group | Confirmed Direct Manager / Integrated | Same |
| Auberge Resorts Collection | Confirmed Direct Manager | Strong lifestyle management candidate |
| Montage International | Integrated / Confirmed Direct Manager | Same |
| Loews Hotels & Co. | Integrated Brand / Operator | Same |
| Omni Hotels & Resorts | Integrated Brand / Operator | Same |
| Meliá Hotels International | Confirmed Direct Manager / Integrated | High CALA/Europe value |
| Barceló Hotel Group | Integrated Brand / Operator | Same |
| RIU Hotels & Resorts | Integrated Brand / Operator | Owner-operator |
| Palladium Hotel Group | Integrated Brand / Operator | Same |
| H10 Hotels | Integrated Brand / Operator | Same |
| Grupo Piñero / Bahia Principe | Integrated Brand / Operator | Same |
| Pestana Hotel Group | Direct Manager — Limited / Integrated | Same |
| Eurostars / Hotusa | Direct Manager — Limited | Europe |
| Sercotel Hotel Group | Franchise / Brand Only or Limited | Confirm model before Master |

### Counts (this audit)

| Metric | Count |
| ------ | ----: |
| Brand Explorer parent companies evaluated (Brand Basics) | **34** (32 meaningful + placeholder/no-parent) |
| Seed list companies evaluated | **35** |
| Confirmed Direct Managers (combined classification) | **10** |
| Direct Manager — Limited / conditional | **6** |
| Integrated Brand / Operators | **16** (incl. seed-not-in-basics) |
| Franchise / Brand Only | **5** |
| Management Availability Unknown | **4+** small Draft parents |
| Not Applicable | **5** (soft brands, HVO, placeholders) |
| Additional brand operators discovered beyond seed (in Brand Basics) | **Sonesta, BWH, Banyan Tree, Preferred, SLH, LHW, Staycity, Red Roof UK, …** — Sonesta/Banyan Tree are the material adds for BM track |

---

## E. Management-scope schema recommendation

### Do **not** assume `Brand Company = Available Manager`

Required scope dimensions (conceptual — dry-run design):

| Dimension | Example |
| --------- | ------- |
| Management entity | Hilton Management Services (alias of Hilton Managed Master) |
| Offered to third-party owners? | Yes / Selective / No / Unknown |
| Brand scope | Conrad, Waldorf Astoria, … (not “all Hilton”) |
| Geography scope | Country / region list or “enterprise excl. X” |
| Hotel / segment scope | Luxury / upper-upscale / resort / … |
| Current management examples | Assignment rows |
| Evidence + last verified | PI Source / Claims |

### Can typed `Operator Intelligence - Brand Relationships` hold this?

**Yes — recommended**, with a relationship type such as:

`Brand Managed Capability`

Example row:

- Operator = Hilton (Managed) / Hilton Management Services  
- Brand = Conrad  
- Relationship Type = Brand Managed Capability  
- Geography Scope = …  
- Evidence = official development page  
- Publication Status = …  

**Does not** imply project approval. Project approval remains Class 3 / outreach.

Optional: if volume/complexity explodes, a thin `Operator Intelligence - Management Offerings` table could later hold productized “managed by brand” programs (MxM) while Brand Relationships holds brand×scope edges. **Not required for Phase 1.**

---

## F. Corporate vs operating entity

| Principle | Recommendation |
| --------- | -------------- |
| One Operator Master per **management offering entity** | e.g. one `Marriott International (Managed)` for MxM — not one Master per Marriott brand |
| Display name | Commercial parent + Managed lens: `Marriott International (Managed)` with subtitle/alias `Managed by Marriott (MxM)` |
| Internal aliases | Registry already maps Parent Company → Master (`brand-managed-operator-link-registry.js`) — extend, don’t duplicate Masters |
| Hilton vs Hilton Management Services | **Same Master**; HMS as alias / management entity label |
| Accor divisions | One Accor (Managed) Master |
| Minor vs NH | **Minor Hotels (Managed)** only; NH brands link via Brand Relationships brand scope |
| Iberostar | Keep `Grupo Iberostar` as Integrated — do not also create `Iberostar (Managed)` |
| Soft brands (Preferred, SLH, LHW) | No Brand-Managed Operator Master |

---

## G. Two-track calibration

### Track 1 — Third-party / regional (12) — unchanged intent

Arbor, Hotel Equities, GHL, Aimbridge LATAM, Playa, Santa Fe, Highgate, Driftwood, Atlantica, Cenote Azul, Iberostar*, Álvarez  

\*Iberostar is **Integrated Brand/Operator** — keep in Track 1 as resort/owner-operator diversity **or** move to Track 2; do not double-count deep research. **Recommendation:** keep Iberostar in Track 1 for CALA resort owner-operator; Track 2 uses other luxury/BM names.

### Track 2 — Brand-managed deep calibration (12)

| # | Company | Class | Master today |
| - | ------- | ----- | ------------ |
| 1 | Marriott International (Managed) / MxM | Confirmed DM | Exists |
| 2 | Hilton (Managed) / HMS | Confirmed DM | Exists |
| 3 | Accor (Managed) | Confirmed DM | Exists |
| 4 | IHG Hotels & Resorts (Managed) | Limited DM | Exists |
| 5 | Hyatt Hotels Corporation (Managed) | Confirmed DM | **Create Research Stage later** |
| 6 | Minor Hotels (Managed) | Confirmed DM | Exists |
| 7 | Sonesta International | Confirmed DM | Create later |
| 8 | Four Seasons | Confirmed DM | Create later |
| 9 | Rosewood Hotel Group | Confirmed DM | Create later |
| 10 | Mandarin Oriental | Integrated | Create later |
| 11 | Radisson Hotel Group | Limited DM | Create later |
| 12 | Meliá Hotels International | Integrated / DM | Create later (not yet in Brand Basics) |
| 13 | Auberge Resorts Collection | Confirmed DM lifestyle | Create later |
| 14 | Shangri-La Group | Integrated | Create later |
| 15 | Barceló Hotel Group | Integrated / CALA-Europe | Create later |

**Deferred deep Assignments** (classify + management-capability discovery only): Wyndham, Choice, BWH, Belmond, Kerzner, Dorchester, Oetker, Langham, Peninsula, Montage, Loews, Omni, RIU, Palladium, H10, Piñero, Pestana, Eurostars, Sercotel, Banyan Tree, Aman.

---

## H. Operator Explorer profile implications

Brand-managed operators use the **same** intelligence model:

Assignments · Market Presence · Brand Relationships (incl. Brand Managed Capability) · Structures · Segment · Development · Evidence  

Candidate-type-specific presentation: Overview labels “Brand-managed”, structures emphasize brand-management agreements, Brand Relationships section shows managed brand scope, best-fit copy contrasts vs pure third-party (already Core 5 pattern).

## I. Operator Fit implications — diagnostic only (no code changes)

Future Fit must compare **pathways**, not collapse entities:

| Pathway | Example |
| ------- | ------- |
| Brand + Brand-Managed Operator | Marriott + Marriott Managed (MxM) |
| Brand + Third-Party Operator | Marriott + Highgate |
| Brand + Third-Party Operator | Marriott + Hotel Equities |

Implications (documentation only):

- Operating structure dimension differs (Brand-managed vs Full third-party)  
- Brand relationship depth differs (self-managed capability vs third-party brand experience)  
- Eligibility/validation: brand-managed availability for **this project** stays Class 3 / Eligible With Conditions until confirmed  
- Scores must not treat MxM and Highgate as interchangeable because both “know Marriott”  
- Shortlist Candidate Type must distinguish Brand-managed vs Third-party  

**No Fit engine changes in this phase.**
