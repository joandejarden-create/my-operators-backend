# Old Home ecosystem label polish — staging QA (2026-07-31)

## Scope
Old Home `/old-home` only. Staging subdomain publish. Production domains not updated.

## Changes
- Card headers: `Brands & Operators`, `Owners & Investors`, `Advisors & Service Partners`
- Close primary: `The Right People. The Right Information. The Right Time.` in Hero Yellow `#fdb52a`
- Removed `Process Can Be Led By` pill (Designer visibility + CSS `display:none`)
- Bullet text left edge aligned via `li::before` flex checkmarks (`v20260731k`)

## Assets
- CSS: `dealality-old-home-ecosystem.v20260731k.css`
- CDN: `https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6ced234857e4ef1b08ca2a_dealality-old-home-ecosystem.v20260731k.css`

## Staging checks
- Headers use `&`
- Pill absent from published HTML
- Close color `rgb(253, 181, 42)`
- Per-card bullet `spanLeft` identical within each card

## Artifacts
`/opt/cursor/artifacts/ecosystem-label-polish-staging-20260731/`
