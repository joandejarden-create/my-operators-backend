# Old Home ecosystem — dashed connectors + centered bottom rows (production)

## Verdict
**Production verified.**

## Publish
| Field | Value |
| --- | --- |
| Timestamp | `2026-07-31T18:29:54.722Z` |
| Payload | `customDomains: ["69c5620ab5bfde0ecff71575","69c56209b5bfde0ecff71549"]`, `publishToWebflowSubdomain: true` |
| CSS | `dealality-old-home-ecosystem.v20260731j.css` |
| URL | https://www.dealality.com/old-home |

## Preflight
- Staging already on `v20260731j` (`2026-07-31T18:23:05.309Z`)
- Old Home head freeform pointed at CSS `j`
- Home `lastUpdated` unchanged: `2026-07-30T13:46:47.675Z`
- Manual Process `1.1.1` present; cinematic absent

## Post-publish QA
Artifacts: `/opt/cursor/artifacts/ecosystem-line-center-prod-qa-20260731/`

| Check | Result |
| --- | --- |
| CSS link | `…/6a6ce7a987c3a9f4525c030b_dealality-old-home-ecosystem.v20260731j.css` |
| Marker | `owner-advisor-led` |
| Manual Process | `1.1.1` |
| Cinematic | false |
| Connector lines | dashed `rgba(120,150,255,.55)` |
| Connector rows | full column width, centered |
| Close panel | `justify-content:center`, `centeredDelta: 0` |
| Root Home lastUpdated | still `2026-07-30T13:46:47.675Z` |
