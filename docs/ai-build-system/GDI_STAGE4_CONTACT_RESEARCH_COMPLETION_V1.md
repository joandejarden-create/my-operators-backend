# GDI Stage 4 Professional Contact Research — Permanent Completion

**Applies to:** Every GDI hotel Full E2E cycle  
**Related:** `gdi-contact-research-stage-v1.js`, `gdi-who-how-agent-contract-v1.js`, `DEALALITY_INTEGRATED_DELIVERY_V1`

## Stage separation (binding)

| Stage | Responsibility |
|---|---|
| **Stage 3 WHO/HOW** | Who to contact; verified identity; professional title; organization; commercial role; buying-authority; opportunity relevance; primary vs alternative entry points |
| **Stage 4 Professional Contact Research** | Professional email; professional telephone; personal LinkedIn; other credible person-specific outreach routes |

Finding a LinkedIn profile **must not** terminate applicable email/telephone research. Document each channel as researched / found / unavailable / unverified / not yet researched.

Do **not** require fabricating every channel. Do **not** require both email and telephone for FQO qualification. Preserve existing FQO reachability rules (one usable route may include LinkedIn).

## Completion requirements

Every future qualified opportunity must have:

1. Completed applicable WHO/HOW research  
2. Completed applicable professional contact-channel research (Stage 4)  
3. Correct identity + buying-authority classification  
4. All available verified contact data persisted through approved mechanisms  
5. Correct STAGED contact hydration (`linkedinUrl` mapped when LinkedIn is in `sourceUrl`)  
6. Cross-surface display parity (List View + Drawer + ES metrics) via shared DTO  

Stage 4 is **not** complete when applicable contact research was never executed.

## Governance

- Surfe: explicit click only; no Full E2E auto; no durable email/phone persistence from Surfe  
- Findymail: separate founder authorization  
- Public-source verified channels may be durably persisted  

## Runners

Reuse existing PowerShell:

```powershell
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 ...
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 ...
```

Rematerialize contact hydration (Wyndham example):

```bash
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --dry-run
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --apply --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_HYDRATION_RECOVERY"
```

Gate: `npm run test:gdi-contact-research-hydration-v1`
