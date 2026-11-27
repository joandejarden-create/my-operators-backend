# GDI Stage 4 Professional Contact Research — Permanent Completion

**Applies to:** Every GDI hotel Full E2E cycle  
**Related:** `gdi-contact-research-stage-v1.js`, `gdi-findymail-stage4-enrichment-v1.js`, `gdi-who-how-agent-contract-v1.js`, `DEALALITY_INTEGRATED_DELIVERY_V1`

## Stage separation (binding)

| Stage | Responsibility |
|---|---|
| **Stage 3 WHO/HOW** | Who to contact; verified identity; professional title; organization; commercial role; buying-authority; opportunity relevance; primary vs alternative entry points |
| **Stage 4 Professional Contact Research** | Professional email; professional telephone; personal LinkedIn; other credible person-specific outreach routes |

Finding a LinkedIn profile **must not** terminate applicable email/telephone research. Document each channel as researched / found / unavailable / unverified / not yet researched.

Do **not** require fabricating every channel. Do **not** require both email and telephone for FQO qualification. Preserve existing FQO reachability rules (one usable route may include LinkedIn).

## Findymail — permanent Stage 4 component (binding)

Findymail is **not** an optional future enhancement. It is a permanent component of authorized Stage 4:

1. Recover existing verified contact intelligence from Airtable / dossiers first.
2. Complete WHO/HOW buyer + alternative entry-point identification.
3. Research professional email, telephone, and LinkedIn (all applicable channels).
4. Use Findymail for unresolved **professional email** gaps within credit/budget controls.
   - **Abbreviated / incomplete surnames** (`Lily Y.`, `Suzanne K.`) are **not** fully verified professional identities and must **not** be submitted for Findymail enrichment (`IDENTITY_INCOMPLETE`). Resolve full identity from verified evidence first, or retain honest PARTIAL identity + alternative entry point.
5. Verify identity + returned email; never invent pattern emails.
6. Persist permitted verified Findymail emails to existing Airtable opportunity contact fields.
7. Hydrate STAGED / List / Drawer from durable stores.
8. Validate List/Drawer parity.

### Findymail vs Surfe

| | Findymail | Surfe |
|---|---|---|
| Full E2E Stage 4 | Permanent component (gated credits) | **Forbidden** auto-call |
| Customer UI | Not a customer click path | Explicit-click only |
| Email/phone persistence | Permitted when verified + authorized | **Never** durable persist |
| Reuse | Cross-opportunity / cross-hotel via personKey ledger | Transient display only |

Client: `lib/group-demand-intelligence/providers/gdi-findymail-client-v1.js`  
Enrichment: `lib/group-demand-intelligence/workflow/gdi-findymail-stage4-enrichment-v1.js`  
Airtable persist: `lib/group-demand-intelligence/persistence/gdi-findymail-airtable-contact-persist-v1.js`  
Auth token: `FOUNDER_GO_GDI_FINDYMAIL_STAGE4_PERMANENT_E2E_20261127`  
Env: `FINDYMAIL_API_KEY`

## Completion requirements

Every future qualified opportunity must have:

1. Completed applicable WHO/HOW research  
2. Completed applicable professional contact-channel research (Stage 4)  
3. Findymail attempted for eligible unresolved professional emails (or documented ineligibility)  
4. Correct identity + buying-authority classification (abbreviated surnames cannot pass as `identityVerified=true`)  
5. All available verified contact data persisted through approved mechanisms  
6. Correct STAGED contact hydration (`linkedinUrl` mapped when LinkedIn is in `sourceUrl`)  
7. Cross-surface display parity (List View + Drawer + ES metrics) via shared DTO  

Stage 4 is **not** complete when applicable contact research was never executed.

## Governance

- Surfe: explicit click only; no Full E2E auto; no durable email/phone persistence from Surfe  
- Findymail: permanent Stage 4; credit ledger + personKey dedupe; Airtable persist for permitted verified emails  
- Public-source verified channels may be durably persisted  

## Runners

```powershell
pwsh -File scripts/gdi/Invoke-GdiWhoHow.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode DryRun
pwsh -File scripts/gdi/Invoke-GdiContactResearch.ps1 -HotelId <rec...> -Mode Live -AuthorizePaidResearch -AuthorizeFindymail
```

Gold-standard certification + Findymail planning/live:

```bash
node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs
node scripts/gdi-canonical-full-e2e-gold-standard-certification-v1.mjs --findymail-live --credit-hard-cap 30
```

Rematerialize contact hydration / identity recovery (Wyndham example):

```bash
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --dry-run
node scripts/gdi-wyndham-contact-hydration-rematerialize-v1.mjs --apply --auth "FOUNDER_GO AUTHORIZE_WYNDHAM_STAGED_CONTACT_ENRICHMENT_COMPLETION"
```

Gates: `npm run test:gdi-findymail-stage4-permanent-v1` · `npm run test:gdi-contact-research-hydration-v1` · `npm run test:gdi-contact-enrichment-actions-v1` · `npm run test:gdi-list-view-contact-actions-v1` · `npm run test:gdi-canonical-full-e2e-gold-standard-certification-v1`
