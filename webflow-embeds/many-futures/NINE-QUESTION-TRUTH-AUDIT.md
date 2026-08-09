# Many Futures — Nine-Question Product Truth Audit

**Status:** Complete before local build. No planned/in-development functionality shown.  
**Webflow:** Not modified. **Publish:** Do not publish.

---

## 1. Confidentiality / owner control (Q05)

| # | Finding |
|---|---------|
| 1 | **Owner-Controlled Process** is a marketing capability name. Live control surface: **Outreach Setup** (`my-deals.html` → Outreach Setup tab) — subsections Confidentiality & targeting, Outreach strategy, Timing & follow-up, Attachments & access. Related: **Deal Room** (grant/revoke, NDA status), **Contacted Brands** status. |
| 2 | **Live confidentiality / information-control fields:** Confidentiality; Identity Disclosure; Exclude/Prioritize companies; Outreach From; Approve Each Message; When to Begin Outreach; Attachments to Include; Attachments Gated (send directly / NDA required / click-to-accept); Allow Forward or Share. Deal Room: NDA Status, Deal Room Access, document Confidentiality (`Public Teaser` \| `NDA Only`). |
| 3 | **Access & Activity Controls** as a distinct third product visual: **Not supportable.** No screen or module with that name. Closest pieces are separate (Outreach attachments, Deal Room ACL, Activity Log). |
| 8–9 | **Q05 layout: two-panel workspace** — Owner-Controlled Process (Outreach Setup HTML reconstruction) + Opportunity Review (existing Deal Brief HTML). No fabricated confidentiality dashboard, digital NDA automation, or invented ACL panel. |

---

## 2. Market Alerts (Q06)

| # | Finding |
|---|---------|
| 4 | **Market Alerts** (`market-alerts.html`) — live screen. Fields/UI: Headlines grid, Live feed, Top read; time windows 24h / 7d / 30d / All; categories (Deals, Capital, Brand, Supply, Demand, Loyalty, Risk); regions; search/saved. Curated RSS hospitality news — batch sync (~240 min default), not streaming. |
| 5 | **Recent Momentum** — Brand Explorer → Footprint & Growth subsection only (`footprint.momentum`). Hint: “Illustrative activity.” Fields: date, headline, description, optional URL; Portfolio Mix pills. **Not** a standalone module. **Not** in Operator Explorer. |
| | **Dealality Radar** — reuse existing approved Radar assets (presence / whitespace / market map). |
| 8–9 | Market Alerts + Recent Momentum → HTML/CSS presentations. Radar → existing images. No predictive alerts, real-time monitoring claims, or invented market-intelligence module. |

---

## 3. Action Tracking / after responses (Q07)

| # | Finding |
|---|---------|
| 6 | **Activity / next-action fields (live):** Contacted Brands — Deal Status, **Next Action**, Activity Log modal, Schedule/Follow-up date, Notes. Brand Opportunity Workspace — Status, Last activity, Follow-up, Next Step, Activity Timeline. Deal Activity Log — Time, Action, Stakeholder, Deal, Details. |
| 7 | **Action Tracking** does **not** exist as a product screen title. Keep as **marketing capability**; discreet interface descriptor: **ACTIVITY LOG & NEXT ACTION** (Contacted Brands / Activity Log). |
| | **Submit Proposal** — brand-only (`brand-deal-request.html`). Descriptor **Brand Response Workflow** already used. Do not imply operators submit. |
| | **Deal Compare** — live structured comparison; reuse existing HTML reconstruction. |
| 8–9 | Primary: HTML Contacted Brands / next-action surface. Support: reuse Submit Proposal + Deal Compare HTML. No fabricated Action Tracking app page. |

---

## Capability that could not be represented truthfully

- **Access & Activity Controls** as a third Q05 panel → omitted (two-panel).
- Digital NDA execution / DocuSign-style automation → not shown.
- Operator Submit Proposal → not shown.
- Predictive / real-time Market Alerts → not claimed.

---

## Optional tenth question (recommendation only — not built)

**How do I preserve a defensible decision record?**

Warranted only if height gap remains **and** activity + comparison history can be shown truthfully. Live pieces exist (Activity Log, Deal Compare) but a dedicated “decision rationale / evidence record” screen does **not**. Prefer **not** adding a tenth question solely for height; revisit only after nine-state height comparison.
