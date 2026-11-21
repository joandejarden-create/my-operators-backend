# Contact Identity Architecture Audit

## PR / Integration status

- **PR #484** — OPEN (not merged). Contact identity preflight extends the same GDI contact-enrichment stream; Integration Controller owns merge.
- Branch: `feat/gdi-google-style-contact-search-before-surfe-v1`
- No competing PR opened.

## Existing components reviewed

| Area | Location | Role vs identity preflight |
|------|----------|----------------------------|
| Canonical contacts store | `lib/group-demand-intelligence/canonical-contacts-store.js` | Hotel-scoped people + opportunity bindings |
| External projection | `canonical-contact-external-projection.js` | Customer-safe contact fields |
| CCI | `gdi-commercial-contact-intelligence-v1` reports + presentation | Commercial relevance / entry-point scoring |
| Google-first discovery | `gdi-google-grounded-contact-discovery-v1.js` | Gemini search + `classifyLinkedInProfileMatch` |
| Public contact discovery | `gdi-public-contact-discovery-v1.js` | Saved → Google → Apify(opt) → Exa path |
| Missing-channel resolver | `gdi-missing-channel-resolver-v1.js` | Now runs identity preflight before Surfe |
| Exa | via PCD / second-pass | Unresolved / conflict cases |
| LinkedIn classification | `LINKEDIN_PROFILE_MATCH` enums | VERIFIED / CANDIDATE / UNCERTAIN / NO_PROFILE |
| Official domain | `native-who-v3/official-domain.js` | Employer domain heuristics |
| Surfe client | `lib/surfe/client.js` | `firstName`, `lastName`, `companyName`, `companyDomain`, `linkedInUrl` |
| Surfe GDI provider | `gdi-surfe-contact-provider-v1.js` | Paid enrich; now accepts companyDomain + blocks untrusted LI |
| Apollo | `gdi-apollo-contact-enrichment-v1.js` | Separate paid path; Free plan blocked |
| Apify | `gdi-apify-google-contact-discovery-v1.js` | LIMITED USE after pilot |
| Airtable / research inventory | slim payload + person source link | Persist public identity; never Surfe-derived into public fields |

## New permanent component

`lib/group-demand-intelligence/presentation/gdi-contact-identity-preflight-v1.js`

- Canonical identity record (name, employer, domain, LinkedIn, relationship, gaps)
- READY / PARTIAL / UNRESOLVED
- Surfe enrichment **input adapter** (no paid calls)
- Reuses LinkedIn classifier + domain resolver — **not** a competing discovery engine

## Boundaries preserved

- Opportunity organizer ≠ person employer (unless explicitly assumed)
- Company LinkedIn pages rejected
- Unverified LinkedIn omitted from Surfe payload
- Publication not blocked solely for missing LinkedIn
- No Surfe-derived persistence into canonical public-contact fields
