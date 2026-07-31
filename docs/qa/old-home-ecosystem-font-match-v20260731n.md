# Old Home ecosystem close primary font match (v20260731n)

## Issue
“The Right People. The Right Information. The Right Time.” did not match the type of How We Do It CTA “One Opportunity. One Connected Process.”

## Fix
Point close primary (`.oh-eco-close-primary`) at the same type recipe as `#oh-how-we-do-it .dealality-process_cta h3`:
- font-family: Plus Jakarta Sans, Inter Tight
- font-weight: 800
- font-size: clamp(1.05rem, 1.6vw, 1.25rem)
- letter-spacing / line-height: normal
- color remains Hero Yellow `#fdb52a`

## Staging
- CSS: `dealality-old-home-ecosystem.v20260731n.css`
- CDN: `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cf2570db3b5d1588eeb9c_dealality-old-home-ecosystem.v20260731n.css`
- Published to webflow.io only (not production custom domains)

## QA (fixture computed styles)
CTA h3 and close primary match on family, weight, and size; close stays `#fdb52a`.
