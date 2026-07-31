# Many Futures — Nine-Question Local Extension Delivery

**Webflow:** not modified · **Publish:** do not publish  
**Branch:** `cursor/many-futures-nine-questions-38e7`

Artifacts: `/opt/cursor/artifacts/many-futures/nine-question-extension/`  
Repo mirror: `webflow-embeds/many-futures/visual-review/nine-question-extension/`

---

## Truth audits (complete before build)

See `NINE-QUESTION-TRUTH-AUDIT.md`.

1. **Confidentiality / owner control** — Outreach Setup + Opportunity Review (two-panel; no Access & Activity Controls)
2. **Market Alerts** — Market Alerts HTML + Radar assets + Brand Explorer Recent Momentum subsection
3. **Action Tracking** — marketing title; descriptor Activity Log & Next Action (Contacted Brands) + Submit Proposal + Deal Compare

---

## Screenshots

| # | File |
|---|------|
| 4 | `01-desktop-1440-q5-confidential.png` |
| 5 | `02-desktop-1440-q6-market.png` |
| 6 | `03-desktop-1440-q7-actions.png` |
| 7 | `05-mobile-390-q5-confidential.png` |
| 8 | `06-mobile-390-q6-market.png` |
| 9 | `07-mobile-390-q7-actions.png` |
| 10 | `08-contact-sheet-desktop-nine-states.png` |
| 11 | `09-contact-sheet-mobile-nine-states.png` |
| 12 | `04-desktop-height-hotel-vs-questions.png` + `height-comparison.json` |

---

## Height comparison (desktop 1440)

| Column | Height |
|--------|--------|
| Hotel / illustrative opportunity | **581px** |
| Nine-question selector | **465px** |
| Delta (questions − hotel) | **−116px** |

Question list is shorter than the hotel column after compact padding. No stretch required.

---

## Transfer weights

| State | Transfer |
|-------|----------|
| Default (Q1 loaded) | **~300 KB** |
| After visiting all nine | **~682 KB** |
| New raster assets for Q5–Q7 | **None** |

Q5–Q7 are HTML/CSS reconstructions; Q6 reuses existing Radar webp/png. Lazy-load of inactive panel images unchanged.

---

## Capability not represented truthfully

- **Access & Activity Controls** as a third Q05 panel — omitted
- Digital NDA automation — not shown
- Operator Submit Proposal — not shown
- Predictive / real-time Market Alerts — not claimed

---

## Optional tenth question

**Not recommended now.**

Height gap does not favor adding a question (selector is already shorter). A “defensible decision record” would need a dedicated rationale/evidence surface that does not exist as a named product screen. Activity Log + Deal Compare are real but insufficient as a tenth owner decision without inventing chrome.
