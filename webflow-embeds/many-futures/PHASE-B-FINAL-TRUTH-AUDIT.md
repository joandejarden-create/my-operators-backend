# Many Futures — Final Phase B Product-Truth & Visual Review

**Status:** Corrected local prototype ready for final visual review.  
**Webflow:** not modified · **Assets:** not uploaded · **Publish:** not done · **Phase C:** do not start until review complete.

**Preview:** `webflow-embeds/many-futures/preview.html`  
**Visual package (repo):** `webflow-embeds/many-futures/visual-review/`  
**Artifacts mirror:** `/opt/cursor/artifacts/many-futures/phase-b-final-review/`

---

## 1. Operator Smart Matching (Question 2) — corrected

**Public panel title:** Smart Matching  
**Interface descriptor:** Operator Match Score & Fit Signals  

**Source live interface:** My Deals → Operator Strategy → `#operatorStrategyTable`  
(`public/my-deals.html`, `public/js/operator-strategy-my-deals.js`, score bands via `DcOperatorMatchScoreUi` / `lib/operator-alignment-scoring-weight-config.js`)

**Visual files:**
- `assets/features/smart-matching-operators-desktop.png`
- `assets/features/smart-matching-operators-mobile.png`

Brand Match Score crop (`smart-matching-desktop/mobile.png`) is retained **only for Question 1** (brand rebrand decision).

---

## 2. Operator Track Record labeling

Hierarchy used in Question 2 third panel:

- Category: **Operator Explorer**
- Name: **Proof & Track Record**
- Purpose: Review relevant operating experience, markets, asset types, brand relationships, and evidence of execution capability
- Benefit: Assess case studies, references, and portfolio evidence from within Operator Explorer—not as a separate application module

Visual remains a distinct crop of the Proof & Track Record tab.

---

## 3. Simplified reconstructions — documentation

Label used only where necessary: **INTERFACE SIMPLIFIED FOR PRESENTATION**  
Not used: CONCEPT INTERFACE / COMING SOON / PROTOTYPE

### A. Smart Matching — operators (`smart-matching-operators-*`)

| Item | Detail |
|---|---|
| Source live interface | Operator Strategy table + Operator Alignment Score |
| Fields retained | Operating Company; Project Location (desktop); Score (`alignmentScoreOptional`); Key Consideration; Alignment Signal band labels; Overall Operator Alignment Score 0–100; nine-factor model note |
| Fields removed | Checkbox; Project / Deal column; Outreach Status; Data Confidence; Call to Action icons; full nine-factor breakdown modal |
| Layout changes | Condensed table for marketing panel; filter chips summarized; mobile drops location column |
| Fictional demo data | Cenote Azul Operadores, Caribe Host Management, Atlántica Hospitality Ops, Litoral Operating Group — labeled “Demo operator record”; illustrative scores/considerations |
| No new function | No fabricated Operator Matching module; no brand columns; scores remain informational fit signals |

### B. Deal Readiness (`deal-readiness-*`)

| Item | Detail |
|---|---|
| Source live interface | Deal Readiness Snapshot |
| Fields retained | Snapshot title; readiness score presentation; completeness / commercial-input style signals; priority gap pattern |
| Fields removed | Full checklist navigation; live deal chrome; actionable edit controls |
| Layout changes | Compact card layout for panel crop |
| Fictional demo data | Illustrative opportunity name / score values |
| No new function | No new readiness algorithms or statuses |

### C. Clause Library (`clause-library-*`)

| Item | Detail |
|---|---|
| Source live interface | Clause Library |
| Fields retained | Search; filter pattern; clause title; agreement type; category; phase; summary; risk / lean badges |
| Fields removed | Full library pagination chrome; deep clause detail drawer |
| Layout changes | Four representative cards in a panel frame |
| Fictional demo data | Illustrative clause titles and summaries consistent with franchise deal vocabulary |
| No new function | No negotiation automation or AI clause scoring |

### D. Financial Term Library (`financial-term-library-*`)

| Item | Detail |
|---|---|
| Source live interface | Financial Term Library |
| Fields retained | Search; category filters; term name; agreement type; category; phase; definition; risk badges |
| Fields removed | Full catalog chrome; admin editing |
| Layout changes | Card grid sized for panel |
| Fictional demo data | Royalty Fee, Marketing / Program Fee, Initial Franchise Fee, Key Money — definitions aligned to known product language |
| No new function | No fee calculation inside the library panel |

### E. Submit Proposal (`submit-proposal-*`)

| Item | Detail |
|---|---|
| Source live interface | Brand Deal Request form (`brand-deal-request.html`) headed **Submit Proposal** |
| Fields retained | Agreement Type; Term & Renewal (Term/Renewal Quantity & Length); Royalty %; Marketing %; Initial Franchise Fee ($); Key Money Amount ($) |
| Fields removed | Full incentive matrix; PIP / Capex; Design Review; Tech fees; Training; Application Fee sub-rows; royalty year schedule grid |
| Layout changes | Two-column condensed form for desktop; stacked for mobile; Brand Response Workflow descriptor |
| Fictional demo data | Illustrative franchise commercial values |
| No new function | Brand-only workflow; no operator submission; no residential fields; no automatic analysis |

### Direct product crops (not reconstructions)

Brand Explorer, Operator Explorer, Operator Track Record (Proof & Track Record crop), Fee Estimator, Dealality Radar, Opportunity Review / Deal Brief, Deal Compare, brand Smart Matching (Q1 only).

---

## 4. Hotel image candidates (for Phase C selection — not yet applied)

**ILLUSTRATIVE OPPORTUNITY** remains permanent.  
**TEMPORARY IMAGE** remains until a final asset is selected.  
Current prototype still uses `hotel-temp.jpg`.

| Rank | File | Rationale |
|---|---|---|
| **1. Recommended** | `hotel-candidate-2-strong-alt.jpg` | ~5 floors (within 4–8); clear canopy + lobby glass; Caribbean/colonial urban street; no brand signage; strongest vertical-crop readiness |
| **2. Strong alternative** | `hotel-candidate-1-recommended.jpg` | Clear operating hotel, canopy, evening arrival, palm context; taller (~9 floors); flags present (monitor for brand implication) |
| **3. Secondary alternative** | `hotel-candidate-3-secondary.jpg` | Strong modern porte-cochère / arrival; tropical landscaping; taller (~9 floors); more contemporary than colonial San Juan street |

Review copies: `visual-review/11-hotel-candidate-*.jpg`

---

## 5. Visual review package checklist

| # | Deliverable | File |
|---|---|---|
| 1 | Desktop Q1 | `01-desktop-1440-q1-rebrand.png` |
| 2 | Desktop Q2 (operator Smart Matching) | `02-desktop-1440-q2-operators.png` |
| 3 | Desktop Q5 | `03-desktop-1440-q5-proposals.png` |
| 4 | Tablet Q1 | `04-tablet-768-q1-rebrand.png` |
| 5 | Mobile Q1 | `05-mobile-390-q1-rebrand.png` |
| 6 | Mobile Q2 (operator Smart Matching) | `06-mobile-390-q2-operators.png` |
| 7 | Mobile Q5 | `07-mobile-390-q5-proposals.png` |
| 8 | Desktop six-state contact sheet | `08-contact-sheet-desktop-six-states.png` |
| 9 | Mobile six-state contact sheet | `09-contact-sheet-mobile-six-states.png` |
| 10 | Simplified reconstruction close-ups | `10-recon-*.png` |
| 11 | Three hotel candidates | `11-hotel-candidate-*.jpg` |

---

## 6. Embed architecture

| Measure | Characters | KB |
|---|---:|---:|
| Readable production source | **47,290** | **46.2** |
| Minified inline Embed | **41,216** | **40.3** |

**Phase C plan:** complete minified inline Embed (approved).  
CDN loader remains backup only if Webflow rejects inline after documented failure + approval.  
Do not strip semantic HTML, ARIA, reduced-motion, progressive enhancement, or keyboard behavior.

---

## Final Q→feature map

| Q | Primary | Supporting | Supporting |
|---|---|---|---|
| 1 Rebrand | Brand Explorer | Smart Matching *(brand Match Score crop)* | Dealality Radar |
| 2 Operators | Operator Explorer | Smart Matching *(operator Match Score)* | Proof & Track Record *(Operator Explorer)* |
| 3 Affiliation | Dealality Radar | Deal Readiness | Fee Estimator |
| 4 Residences | Opportunity Review | Brand Explorer | Dealality Radar |
| 5 Proposals | Deal Compare | Submit Proposal | Fee Estimator |
| 6 Clarify | Deal Readiness | Clause Library | Financial Term Library |

---

## Stop point

Await visual review of this package before any Webflow implementation or Phase C.
