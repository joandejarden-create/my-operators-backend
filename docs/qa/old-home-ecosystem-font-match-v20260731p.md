# Ecosystem type match to How We Do It CTA (v20260731p)

## Issue
- Close primary “The Right People…” was Hero Yellow, not white like How We Do It CTA “One Opportunity. One Connected Process.”
- Body, bullets, and close support were not explicitly locked to the Inter Tight body recipe used across Old Home.

## Fix (`dealality-old-home-ecosystem.v20260731p.css`)
Close primary = CTA h3 recipe:
- Plus Jakarta Sans / 800 / `clamp(1.05rem,1.6vw,1.25rem)` / **white `#fff`**

Close support = CTA p recipe:
- Inter Tight / `.84rem` / `line-height:1.45` / `rgba(255,255,255,.58)`

Body/bullets/lead/step body:
- Inter Tight / body sizes matching FAQ + platform feature copy

Card + step titles:
- Plus Jakarta Sans (same family as other section titles)

## Staging
- CDN: `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6cf512ee14e411dfccd488_dealality-old-home-ecosystem.v20260731p.css`
- MD5: `1366bd499001e170d01b7d0d4822263c`
- Published to webflow.io only (not production custom domains)
