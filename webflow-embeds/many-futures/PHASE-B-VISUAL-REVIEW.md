# Many Futures — Phase B Visual Review Package

**Status:** Corrected local prototype ready for visual review.  
**Webflow:** not modified · **Publish:** not done · **Phase C:** do not start until screenshots are reviewed.

**Branch:** `cursor/many-futures-phase-b-38e7`  
**Local preview:** `webflow-embeds/many-futures/preview.html`  
**Artifacts:** `/opt/cursor/artifacts/many-futures/phase-b-visual-review/`

---

## 1. Corrected Question 2 mapping

**Which operators genuinely fit this hotel?**

| Role | Feature | Notes |
|---|---|---|
| Primary | **Operator Explorer** | Operator identity, structure, footprint |
| Supporting | **Smart Matching** | Match Score & Fit Signals capability |
| Supporting | **Operator Track Record** | Distinct **Proof & Track Record** view *inside* Operator Explorer — not a standalone app module |

Panel treatment for the third feature:

- Category: **Operator Explorer**
- Name: **Operator Track Record**
- Purpose: Review relevant experience and operating evidence
- Benefit: Assess hotel types, markets, brand relationships, references, and case studies from the Proof & Track Record view

**Deal Readiness removed** from Question 2 (not forced as a third panel filler).

Layout still supports a two-panel workspace via `.mf-features--duo` if a future review prefers Operator Explorer + Smart Matching only.

---

## 2. Corrected Question 4 mapping

**Could branded residences strengthen the project?**

| Role | Feature | Notes |
|---|---|---|
| Primary | **Opportunity Review** | Deal Brief interface |
| Supporting | **Brand Explorer** | Brand positioning / owner fit |
| Supporting | **Dealality Radar** | Market, location, nearby supply, demand, brand presence |

**Submit Proposal removed from Question 4** — the live Brand Deal Request form has **no residential / mixed-use fields**.

No Branded Residences module, residential scores, partner rankings, or residential-specific submission fields.

**Owner outcome:** Understand whether a residential component merits deeper evaluation as part of the wider asset strategy.

---

## 3. Exact verified Submit Proposal fields used in the visual

**Product name:** Submit Proposal  
**Context:** Brand Deal Request form  
**Descriptor:** Brand Response Workflow  

Used primarily on **Question 5** (Deal Compare → Submit Proposal → Fee Estimator).

Verified live form fields represented in the simplified panel:

- Agreement Type
- Term & Renewal (Term Quantity / Length, Renewal Quantity / Length)
- Royalty %
- Marketing %
- Initial Franchise Fee ($)
- Key Money Amount ($)

Demo numeric values are illustrative placeholders only.

**Not implied:** operator submission through this workflow; all partner types sharing one process; branded-residences-specific questions; automatic analysis beyond current functionality.

---

## 4. Primary breakpoint screenshots

| # | File |
|---|---|
| 1 | `01-desktop-1440-q1-rebrand.png` |
| 2 | `02-desktop-1440-q2-operators.png` |
| 3 | `03-desktop-1440-q5-proposals.png` |
| 4 | `04-tablet-768-q1-rebrand.png` |
| 5 | `05-mobile-390-q1-rebrand.png` |
| 6 | `06-mobile-390-q5-proposals.png` |

---

## 5–6. Contact sheets (review only — not for Webflow)

| Sheet | File |
|---|---|
| Six desktop question states | `contact-sheet-desktop-six-states.png` |
| Six mobile question states | `contact-sheet-mobile-six-states.png` |

---

## 7. Updated visual-truth audit

| Feature visual | Source / type | Truth notes |
|---|---|---|
| Brand Explorer | Product crop | Live Brand Explorer |
| Operator Explorer | Product crop | Live operator identity / metrics |
| Operator Track Record | Product crop | Proof & Track Record tab in Operator Explorer |
| Smart Matching | Product crop | Existing Match Score / Preferred Brand table; marketing capability, not a fabricated app |
| Dealality Radar | Product crop | Live Radar market/map UI |
| Opportunity Review | Product crop | Deal Brief interface; marketed as Opportunity Review |
| Deal Compare | Product crop | Live commercial comparison |
| Fee Estimator | Product crop | Live fee results |
| Deal Readiness | Simplified reconstruction | Used on Q3 / Q6 only; labeled for presentation |
| Clause Library | Simplified reconstruction | Q6; labeled for presentation |
| Financial Term Library | Simplified reconstruction | Q6; labeled for presentation |
| Submit Proposal | Simplified reconstruction | Verified brand form fields; Brand Response Workflow; Q5 only |

### Final question → feature map

| Q | Primary | Supporting | Supporting |
|---|---|---|---|
| 1 Rebrand | Brand Explorer | Smart Matching | Dealality Radar |
| 2 Operators | Operator Explorer | Smart Matching | Operator Track Record |
| 3 Affiliation | Dealality Radar | Deal Readiness | Fee Estimator |
| 4 Residences | Opportunity Review | Brand Explorer | Dealality Radar |
| 5 Proposals | Deal Compare | Submit Proposal | Fee Estimator |
| 6 Clarify | Deal Readiness | Clause Library | Financial Term Library |

---

## 8–11. Embed size and CDN decision

| Measure | Characters | KB |
|---|---:|---:|
| Readable production source (CSS + markup + JS, unminified) | **47,153** | **46.0** |
| Minified Webflow inline Embed | **41,079** | **40.1** |
| Webflow Code Embed limit (approx.) | ~50,000 | ~50 |
| Headroom target | &lt; 45 KB minified | **Pass (40.1 KB)** |

**Phase C default:** use the **complete minified inline Embed**.  
CDN loader retained only as **backup** (`cdn-loader.html`, ~0.7 KB) if Webflow rejects the inline embed after a validated insertion test and explicit approval.

Semantic HTML, ARIA, reduced-motion, and progressive enhancement retained.

---

## 12. Visual hierarchy confirmation

For each active state, intended hierarchy:

1. Primary Dealality capability (largest panel, strongest frame)
2. Supporting Dealality capabilities
3. Active owner question (compact list + Active badge)
4. Owner outcome (beneath workspace)
5. Illustrative opportunity (context; narrower column, quieter title)
6. Inactive questions (compact, muted)

Question buttons contain number + concise title only. Fuller decision copy lives in the workspace header.

---

## Unresolved visual / product limitations

1. **Q2 Smart Matching visual** reuses the existing Match Score table crop sourced from matched-brands (Preferred Brand + Match Score). An in-app Operator Match Score UI exists; a dedicated operator-match crop was not available for this package.
2. **Operator Track Record** is correctly framed as an Operator Explorer view, not a separate application.
3. **Submit Proposal** remains brand-only; operator submission is not live.
4. **Deal Readiness / Clause Library / Financial Term Library / Submit Proposal** visuals are simplified reconstructions labeled for presentation.
5. **Hotel image** remains temporary.
6. Contact sheets are for review only and must not be added to Webflow.

---

## Stop point

Phase B corrections and visual-review package are complete.  
Await screenshot review before any Webflow work or Phase C.
