# Implementation table — SHARED INTERNATIONAL PHONE FORMATTING V1

| Product | Component | Source File | Shared Formatter Applied | Dialing Link Validated | Raw Value Preserved | Browser Validation |
|---|---|---|---|---|---|---|
| Shared | Canonical util | `lib/dealality-shared/dealality-shared-international-phone-formatting-v1.js` | Yes | Yes | Yes | Unit 9/9 |
| Shared | Browser facade | `public/js/dealality-shared-international-phone-formatting-v1.js` | Yes | Yes | Yes | Playwright eval |
| GDI | Drawer + tiles + Surfe + whoHow | `dealality-gdi-ui.js` | Yes | Yes | Yes | Playwright |
| GDI | List Contact Details | `gdi-commercial-list-table-v1.js` | Yes | Yes | Yes | Playwright |
| HE | Contact Intelligence | `hotel-contact-intelligence.js` | Yes | Yes | Yes | Shared unit parity |

## Bethesda golden

| Raw | Display | tel: |
|---|---|---|
| `+12674754267` | `+1 (267) 475-4267` | `tel:+12674754267` |
| `+19722335600` | `+1 (972) 233-5600` | `tel:+19722335600` |
