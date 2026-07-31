# Old Home Manual Process — Staging Release Candidate

**Recommendation: Ready for user production approval**

Production was **not** published.

## 1. Staging revision

| Field | Value |
| --- | --- |
| Staging publish timestamp | `2026-07-31T17:56:47.825Z` |
| Payload | `customDomains: []`, `publishToWebflowSubdomain: true` |
| Staging URL | https://mvp-deal-capture.webflow.io/old-home |
| www.dealality.com lastPublished | `2026-07-31T15:18:27.790Z` (unchanged) |
| dealality.com lastPublished | `2026-07-31T15:18:27.790Z` (unchanged) |
| Root Home lastUpdated | `2026-07-30T13:46:47.675Z` (unchanged) |

### Manual Process package

| Asset | Version / ID |
| --- | --- |
| Section | `#about[data-oh-problem="manual-process"]` (exactly once) |
| HtmlEmbed | `#oh-manual-process-embed` `data-oh-manual-process="1.1.1"` |
| Shell CSS | `old-home-manual-process.shell.v20260731a.css` |
| Section CSS | `old-home-manual-process.v20260731d.css` |
| Boot | `old-home-manual-process.boot.v20260731b.js` |
| HTML payload | `old-home-manual-process.v20260731b.html` (`data-dmp-version="1.1.1"`) |
| Draw JS | `old-home-manual-process.v20260731a.js` |
| Ecosystem | `dealality-old-home-ecosystem.v20260731i.css` + `owner-advisor-led` |

## 2. Unpublished deltas after `2026-07-31T17:29:13.149Z`

Compared published staging HTML (17:29) vs Designer before this RC publish.

### Kept — Approved Manual Process + three-card copy

**Node:** HtmlEmbed `#oh-manual-process-embed`  
**Element ID:** `{component:68108c2a063eeb5d1bd7ae90, element:a64ef2f7-2f5f-ab92-9711-5f43f9eeb3fa}`

| Field | Staging @ 17:29 | Designer / RC |
| --- | --- | --- |
| Attribute `data-oh-manual-process` | `1.1` | `1.1.1` |
| CSS | `…v20260731a.css` | `…v20260731d.css` |
| Boot | `…boot.v20260731a.js` → HTML `v20260731a.html` | `…boot.v20260731b.js` → HTML `v20260731b.html` |
| Three-card copy | Old (“Fragmented Process” / “Comparison Weakened” / “Value Left Unseen”) | Approved three-card copy (exact) |

Classification: **Approved Manual Process change** + **Approved three-card copy change**.

### Reverted — Accidental / superseded

**Location:** Old Home page freeform footer  
**Change:** Removed obsolete Deal Desk Phase B script  
`old-home-problem-deal-desk.v1b.js`  
Classification: **Unrelated/superseded cinematic script** — removed for clean Manual Process RC.

### Not changed

- Root Home `/`
- Routes, redirects, domains, CMS, forms
- Site applied scripts (`lastUpdated` still `2026-07-31T15:18:06.916Z` before this edit window; no site-script list changes for this RC)
- Site freeform head/footer (except no site-level Deal Desk restore)
- Many Futures HtmlEmbed
- Ecosystem page-head CSS `v20260731i`
- `#platform-features` Designer node remains `visibility:false` and is absent from both staging and production published HTML (unchanged)

## 3. Updated production gate definition (Manual Process)

Obsolete cinematic gates removed. Required gates:

1. `#about[data-oh-problem="manual-process"]` exactly once  
2. Old cinematic Deal Desk hidden/removed (`data-visual="cinematic-v1"` / deal-desk script absent)  
3. No duplicate problem-section copy  
4. Five opportunity paths visible  
5. Incoming paths visibly cross and mix  
6. Manual Process includes Email / Spreadsheets / Separate conversations  
7. Exactly one solid outgoing path connects  
8. Several dotted outgoing paths unresolved  
9. Dotted paths contain no circles  
10. Dotted paths contain no X marks  
11. Selected Direction includes exactly three questions  
12. Three approved problem cards present (exact copy)  
13. Closing question appears exactly once  
14. CTA → `https://www.dealality.com/opportunity-review`  
15. Reduced motion shows complete static meaning  
16. No horizontal overflow  
17. No section console errors from Manual Process  
18. Many Futures unchanged  
19. `#platform-features` unchanged (Designer hidden; not published — same as production)  
20. Root Home unchanged  

## 4. Staging QA summary

Widths: 1440 / 1200 / 768 / 390 / 320 — Manual Process section captured.  
Reduced motion: complete static meaning + cards + CTA present.  
Ecosystem `owner-advisor-led` + `v20260731i` present.  
Many Futures present.  
Deal Desk script / cinematic hosts: absent.  
Overflow-X: false.  
Console: pre-existing globe CORS + analytics abort noise only; no Manual Process section errors.  
Production domains: not published.

Artifacts: `/opt/cursor/artifacts/manual-process-staging-qa-20260731/`

## 5. Production preflight (vs Manual Process RC)

| Gate | Result |
| --- | --- |
| Gate 1 unpublished audit vs this RC | Pass after staging publish — Designer package matches hosted staging |
| Gate 2 Manual Process parity | Pass on staging |
| Gate 3 root safety | Home unchanged; prod domains still `15:18:27.790Z` |
| Gate 4 domain selection | Not executed (no production publish) |

**Production publish: not executed.**

## 6. Recommendation

**Ready for user production approval** of the Manual Process staging RC at `2026-07-31T17:56:47.825Z`.

Do not publish production until explicit user approval.
