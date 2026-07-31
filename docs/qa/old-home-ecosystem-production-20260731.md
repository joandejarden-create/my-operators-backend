# Old Home ecosystem — production publish (v20260731r)

## Verdict
**Production verified** on www + apex.

## Publish
| Field | Value |
| --- | --- |
| Timestamp | `2026-07-31T19:31:57.224Z` |
| Payload | `customDomains: ["69c5620ab5bfde0ecff71575","69c56209b5bfde0ecff71549"]`, `publishToWebflowSubdomain: true` |
| CSS | `dealality-old-home-ecosystem.v20260731r.css` |
| CDN | `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cf78bd550ecff473686b0_dealality-old-home-ecosystem.v20260731r.css` |
| CSS MD5 | `d2c3a4694bacd1f1ccdcb122adf06862` |
| URLs | https://www.dealality.com/old-home · https://dealality.com/old-home |

## Guardrails
| Check | Result |
| --- | --- |
| Root `/` still Home | yes (`publishedPath: /`, title Dealality \| Hotel Owner…) |
| Home `lastUpdated` | unchanged `2026-07-30T13:46:47.675Z` |
| Old Home slug | `/old-home` |
| Domains published | www + apex + Webflow subdomain |

## Post-publish QA
| Check | Result |
| --- | --- |
| CSS `v20260731r` linked | true (www + apex) |
| Lead copy | “…market themselves, engage, respond, and move opportunities forward.” |
| Close support | “Dealality keeps every opportunity organized, aligned, and moving at the owner's pace.” |
| Close primary type | Plus Jakarta Sans / 800 / clamp; brand gold `#fdb52a` |
| Process titles | `One Hotel<br/>Opportunity` · `Owner or Advisor<br/>Leads the Process` · `Better-Aligned<br/>Decision` |
| Process arrows | 2× `.oh-eco-step-arrow` (`>`) between steps |
| Ecosystem marker | `data-oh-ecosystem="owner-advisor-led"` |
