# Operator Setup — Full Table Inventory

**Generated:** 2026-08-10T16:41:20.538Z
**Operator Setup tables:** 14
**Total Setup fields:** 597

| Table | ID | Records | Fields | Linked to Master? | Purpose | Consumers | Writers | Status |
| ----- | -- | ------: | -----: | ----------------- | ------- | --------- | ------- | ------ |
| Operator Setup - Master | `tbl4YPJ3XhnYLHLsD` | 46 | 56 | No | Canonical operator identity + lifecycle + OE readiness sync | OE universe; Operator Fit eligibility (status); Explorer; PI governance | operator-setup writers; OE wave scripts; intake forms | Active |
| Operator Setup - Profile & Positioning | `tblNHCPoYI7Dz23wK` | 36 | 68 | Yes | Company profile, branding, chain scales, service model, overview narratives | Operator Explorer; Operator Fit (chainScales, brands); DNA explorer JSON | new-base writer; profile-deepen; website-content-apply; linked-tabs-bootstrap | Partially Active |
| Operator Setup - Platform & Markets | `tblJ9uTS6vzCZ5248` | 36 | 131 | Yes | Geography, scale, development experience, market narratives | Operator Fit (Active Countries); Explorer; DNA JSON | new-base writer; website-content-apply; linked-tabs-bootstrap | Partially Active |
| Operator Setup - Commercial Fit & Terms | `tblkWAQHmTZRQGZES` | 36 | 165 | Yes | Commercial terms, owner engagement, fee structures, Fit preferences | Operator Fit (structures, commercial prefs); Explorer | new-base writer; intake; website-content-apply | Partially Active |
| Operator Setup - Governance, Delivery & Diligence | `tblygTlNhFqyhU1Wu` | 33 | 81 | Yes | Governance, delivery, diligence narratives and controls | Explorer; intake QA | new-base writer; website-content-apply | Partially Active |
| Operator Setup - Case Studies | `tblAh1X0KDK8SeYK0` | 58 | 13 | Yes | Historical operator case-study stories | Legacy Explorer / Fit project-experience hints | legacy intake / deepen packs | Legacy |
| Operator Setup - Brand Relationships | `tblWU8UDz2pVh3ss4` | 73 | 8 | Yes | Explorer section rows for brand relationship storytelling (NOT intel BR table) | Operator Explorer UI sections | Explorer content packs / normalize brands | Partially Active |
| Operator Setup - Leadership Team Members | `tbl8jX7BoOcwUIEOd` | 58 | 15 | Yes | Named leadership people | Explorer leadership tab | intake / deepen | Partially Active |
| Operator Setup - Diligence QA | `tbl5kF0qGg7X6FtCr` | 110 | 5 | Yes | Diligence checklist / QA workflow rows | Internal QA | intake / QA scripts | Workflow |
| Operator Setup - Explorer Materials | `tbllzayIeyvlRzHdz` | 221 | 22 | Yes | Explorer materials / gallery / presentation artifacts | Operator Explorer materials | materials pipeline | Partially Active |
| Operator Setup - Engagement & Reporting | `tblBwfXeyeZ5mQd3T` | 548 | 8 | Yes | Explorer section rows — engagement & reporting narratives | Operator Explorer UI | content packs | Partially Active |
| Operator Setup - Infrastructure Platform | `tblWOPwYIIlnNBZ1w` | 52 | 8 | Yes | Explorer section rows — infrastructure platform | Operator Explorer UI | content packs | Partially Active |
| Operator Setup - Leadership Platform | `tblhsiVJkVU6H7GVT` | 57 | 8 | Yes | Explorer section rows — leadership platform | Operator Explorer UI | content packs | Partially Active |
| Operator Setup - Operating Platform | `tbl3ARHs2Gl187YDc` | 584 | 9 | Yes | Explorer section rows — operating platform capabilities | Operator Explorer UI | content packs / deepen | Partially Active |

## Counts

- Active / Partially Active: 12
- Legacy: 1
- Workflow: 1

Also related (not named Operator Setup but Fit/OE): Operator Fit - Shortlist, Operator Intelligence -*, Operator Deal Requests.
