# Brand Explorer Factory — Image Integrity Audit V1

**Generated:** 2026-11-20T19:25:00.837Z
**Mode:** AUDIT_DRY_RUN
**Brands audited:** 40

## Correction disclosure

Previous factory scoring treated URL population as image completion. Certified Image Completion only counts CERTIFIED slots after fetch+decode(+browser) + visual validation.

| Metric | Value |
|---|---|
| Previously claimed (URL population) avg | 75.0% |
| Certified completion before recovery avg | 0.0% |
| Certified completion after avg | 0.0% |
| Brands fully certified | 0 |
| Broken images | 89 |
| Missing images | 90 |
| Visual review required | 100 |
| Technical recovery success rate | n/a |

## AC Hotels by Marriott

{
  "legacy": 1,
  "certifiedBefore": 0,
  "certifiedAfter": 0,
  "broken": 7,
  "missing": 0,
  "rootCauses": [
    {
      "slotKey": "materials.gallery.1",
      "status": "RENDER_VALID",
      "reason": "awaiting_browser_render_certification",
      "failureClass": null,
      "url": "https://cache.marriott.com/is/image/marriotts7prod/gdlac-entrance-0001:Wide-Ver?wid=377&fit=constrain"
    },
    {
      "slotKey": "materials.gallery.2",
      "status": "DECODE_FAILED",
      "reason": "unsupported_or_corrupt_format",
      "failureClass": "technical",
      "url": "https://cache.marriott.com/content/dam/marriott-renditions/GDLAC/gdlac-guestroom-0007-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*"
    },
    {
      "slotKey": "materials.gallery.3",
      "status": "DECODE_FAILED",
      "reason": "unsupported_or_corrupt_format",
      "failureClass": "technical",
      "url": "https://cache.marriott.com/content/dam/marriott-renditions/GDLAC/gdlac-guestroom-0006-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*"
    },
    {
      "slotKey": "materials.gallery.4",
      "status": "DECODE_FAILED",
      "reason": "unsupported_or_corrupt_format",
      "failureClass": "technical",
      "url": "https://cache.marriott.com/content/dam/marriott-renditions/GDLAC/gdlac-lobby-0015-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*"
    },
    {
      "slotKey": "materials.gallery.5",
      "status": "RENDER_VALID",
      "reason": "awaiting_browser_render_certification",
      "failureClass": null,
      "url": "https://cache.marriott.com/content/dam/marriott-renditions/GDLAC/gdlac-lobby-0015-hor-feat.jpg?output-quality=70"
    },
    {
      "slotKey": "materials.gallery.6",
      "status": "DECODE_FAILED",
      "reason": "unsupported_or_corrupt_format",
      "failureClass": "technical",
      "url": "https://cache.marriott.com/content/dam/marriott-renditions/GDLAC/gdlac-desk-0037-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*"
    },
    {
      "slotKey": "overview.scenario.1",
      "status": "FETCH_FAILED",
      "reason": "http_410",
      "failureClass": "technical",
      "url": "https://v5.airtableusercontent.com/v3/u/59/59/1795176000000/Q8sgnPRfQaRlfwAasD6Pxw/TSoBp98VAWQE_4FVyvQn3zNYl29heSThko2DKGTUeJWSRFTYUc50qRXedp3VU-U69r_OQU7aHnWKAbWJRB1jlCYCyp0dlxvrzWQAMCyxmnFaCqhZjYL04_9Y-FrYT1i0WcICumSFxemf5AR4AOySxA/gzcGWOin-iRVGvsFbUHPU437-YBiXT_YGbn3TIWQAjU"
    },
    {
      "slotKey": "overview.scenario.2",
      "status": "DECODE_FAILED",
      "reason": "unsupported_or_corrupt_format",
      "failureClass": "technical",
      "url": "https://cache.marriott.com/content/dam/marriott-renditions/GDLAC/gdlac-guestroom-0007-hor-wide.jpg?output-quality=70&interpolation=progressive-bilinear&downsize=1336px:*"
    },
    {
      "slotKey": "overview.scenario.3",
      "status": "FETCH_FAILED",
      "reason": "http_410",
      "failureClass": "technical",
      "url": "https://v5.airtableusercontent.com/v3/u/59/59/1795176000000/lptlBnqRjlyyiwB2ZdWwFw/wbcEskxTY85LSe5_43qeCp765r-OiqwB1DPW4fqZ6c9mfBu1a17ozT4VVp_850dB-UqWfgcXZprNxPvTP-8fBKo9wiSrG2U9Vs0FFZmDDBXyO1LXMebiRSf9ZW-oWtq5HjBmpwFtbP6p3zv-GlR2mw/qBXkV4wAy4yVgfd7C3y6MfZMJl_99qcclcfvUiOBHZU"
    }
  ]
}

## STOP

No merge, deployment, paid research, destructive persistence, or customer publication in this run.
