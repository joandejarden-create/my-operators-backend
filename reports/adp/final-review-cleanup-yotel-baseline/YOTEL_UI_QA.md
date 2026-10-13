# YOTEL UI QA

## Publication
- Property: **YOTEL Geneva Lake** (`adp_yotel_geneva_lake`)
- Period: `adp_period_adp_yotel_geneva_lake_20261005144555_b21d18`
- Manifest `certificationStatus`: **CERTIFIED**
- `publishStatus`: Live
- Official published: **YES**
- `customerDropdownVisible`: **false** (prospect — dropdown intentionally deferred; report file + PDF path verified)

## Verified from published report payload
| Check | Result |
|-------|--------|
| Property identity | YOTEL Geneva Lake / brand YOTEL / Founex / Lake Geneva / La Côte |
| Period date | 2026-10-05 (measurement) |
| Scenario count | 63 (57 captured → Scenario Presence 90.5%) |
| Provider count | 4 (openai, gemini, perplexity, claude) |
| AI Consideration | 35.1% (88/251) |
| Owned source | yotel.com as Top Owned/Brand Source (40.6% share) |
| Top source supporting property | tripadvisor.com (PROPERTY_SPECIFIC_EXTERNAL) |
| Top competitive-universe source | hilton.com (COMPETITOR_OWNED) |
| Internal QA/debug copy in customer payload | **None** (`hasDebug=false`) |
| Stale periods | Single latest period in manifest — no stale overwrite |

## Browser
- PDF/owner shells require Memberstack sign-in (observed: auth gate on `/adp-current-report-pdf-render.html?propertyId=adp_yotel_geneva_lake`).
- Customer payload QA completed from published `report-*.json` + `manifest.json` (CERTIFIED, identity, metrics, source taxonomy labels, no debug copy).
- Owner dropdown surface: deferred until `customerDropdownVisible=true` (prospect policy).
