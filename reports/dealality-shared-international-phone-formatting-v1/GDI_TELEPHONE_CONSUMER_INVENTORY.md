# GDI telephone display consumer inventory

Contract: `DEALALITY_SHARED_INTERNATIONAL_PHONE_FORMATTING_V1`

| Product | Component | Source File | Shared Formatter Applied | Dialing Link Validated | Raw Value Preserved | Browser Validation |
|---|---|---|---|---|---|---|
| Shared | Node formatter | `lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js` | Yes (canonical) | Yes | Yes | Unit |
| Shared | Browser facade | `public/js/dealality-shared-international-phone-formatting-v1.js` | Yes | Yes | Yes | Playwright eval |
| Shared | Vendor | `public/vendor/libphonenumber-js.min.js` | libphonenumber-js@1.13.17 | — | — | Loaded |
| GDI | Drawer / contact summary / Surfe / whoHow / company phone / backups / tile channel | `public/js/group-demand-intelligence/dealality-gdi-ui.js` | Yes (`phoneAnchorHtml` / `formatPhonePresentation`) | Yes | Yes | Playwright |
| GDI | List View Contact Details | `public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js` | Yes | Yes | Yes (raw in `phone`, display in `phoneDisplay`) | Playwright |
| GDI | HTML shell | `public/group-demand-intelligence.html` | Scripts wired | — | — | Playwright |
| GDI | Share shell | `public/group-demand-intelligence-share.html` | Scripts wired | — | — | Source |
| HE | Contact Intelligence | `public/js/hotel-contact-intelligence.js` | Yes | Yes | Yes | Source + shared unit |
| HE | Preview / Radar shells | `hotel-explorer-preview.html`, `deal-capture-radar-with-ranked-list.html` | Scripts wired | — | — | Source |

## Not migrated (rationale)

| Component | Rationale |
|---|---|
| Executive Summary metrics labels (`Contacts with verified telephone`) | Counts only — no telephone values displayed |
| Research / Sales Performance admin | No telephone value displays found |
| Portfolio Demand Intelligence tab JS | No direct telephone display strings found; opportunities reuse GDI UI drawer/list |
| CSV export fields | Contract preserved — raw values unchanged |

## Library

- **libphonenumber-js** `^1.13.17`
- Docs: https://github.com/catamphetamine/libphonenumber-js
