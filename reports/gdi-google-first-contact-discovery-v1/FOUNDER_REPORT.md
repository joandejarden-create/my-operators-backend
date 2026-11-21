# GDI — Google AI first, Exa second, Surfe last

**Date:** 2026-11-20  
**PR:** [#484](https://github.com/joandejarden-create/my-operators-backend/pull/484) — OPEN (extends same resolver)  
**Branch:** `feat/gdi-google-style-contact-search-before-surfe-v1`  
**Workspace:** `C:\Dev\dealality\ws-gdi`

## 1. Google API readiness

| Check | Result |
|-------|--------|
| `GEMINI_API_KEY` | **PRESENT** (Integration `.env`; value not logged) |
| Grounding tool | `tools: [{ google_search: {} }]` via existing `lib/ai-visibility/providers/gemini.js` |
| Model used (live) | `gemini-3.6-flash` |
| Consumer AI Mode scrape | **NO** |
| Serper | Not configured |
| Exa | Present (secondary) |
| Chosen mode | `GOOGLE_GEMINI_THEN_EXA` |

Grounding **works** (live Islas returned webSearchQueries + source URL candidates).  
**Parity with consumer Google AI Mode is NOT claimed** — official Gemini Search grounding only.

## 2. Provider order (canonical)

1. Saved verified Airtable / GDI intelligence  
2. **Google Gemini + Search grounding** (PRIMARY) → candidate URLs only  
3. Authoritative page fetch + named-person extraction  
4. **Exa** (SECONDARY) only if channels still missing  
5. Persist verified public channels  
6. **Surfe** LAST, explicit paid authorization, session-only  

Module: `gdi-google-grounded-contact-discovery-v1.js`  
Resolver: same `gdi-missing-channel-resolver-v1.js` (no second engine)

## 3. LinkedIn discovery

Statuses: `VERIFIED_LINKEDIN_PROFILE` | `CANDIDATE_LINKEDIN_PROFILE` | `PROFILE_MATCH_UNCERTAIN` | `NO_PROFILE_FOUND`  

Only **VERIFIED** is shown as the individual LinkedIn profile. Name-only matches are never auto-verified. No restricted LinkedIn scrape.

## 4. Three-case results

### Offline

- `npm run test:gdi-google-first-contact-discovery-v1` → **PASS** (Google before Exa; Surfe skipped)  
- `npm run test:gdi-three-case-public-contact-search-before-surfe-v1` → **PASS**

| Contact | Email | Phone | Extensions |
|---------|-------|-------|------------|
| Islas | fislas@hcg.gob.mx | +52 33 3883 4400 | 51621 |
| Maritza | maritza.medina@jalisco.gob.mx | +52 33 3819 2333 | 42333 / 47542 |
| Alejandra | alejandra.vazquez@igeco.mx | none | — (+ Paulina Sánchez secondary) |

### Live Google-first (Islas)

Evidence: `LIVE_GOOGLE_FIRST.json`

- Google grounded call: **ok**, groundingUsed=true, 6 source URL candidates  
- Exa secondary: 1 query (~$0.007) because first-pass verify was incomplete (likely redirect/TLS ordering)  
- Final verified: **fislas@hcg.gob.mx**, **+52 33 3883 4400**, **Ext. 51621**  
- LinkedIn: `NO_PROFILE_FOUND`  
- Surfe: not called  
- Google token cost: **COST_UNKNOWN** (pricing not calibrated; usage recorded when returned)

## 5. Metrics (this certification)

| Metric | Value |
|--------|-------|
| Named emails recovered (Islas live) | 1 |
| Office phones + extensions | 1 + Ext. 51621 |
| Verified LinkedIn | 0 (Islas) |
| Exa calls avoided when Google URL mock-verified | yes (offline) |
| Surfe lookups avoided | yes |
| Google incremental vs Exa-only | Google now primary; Exa still useful as verify fallback |
| Airtable apply three-case | **PENDING_FOUNDER_AUTH** |
| Local/Railway drawer screenshots | **PENDING** Integration merge + deploy |

## 6. Remaining limitations

- Gemini grounding often returns redirect/wrapper URLs — prioritization added; Exa still needed when first fetch fails.  
- Consumer Google AI Mode experience ≠ API grounding; do not declare parity.  
- No new Google budget beyond existing visibility key for this certification.  
- Shared PDI/HE/Radar contact-card parity: reuses existing LinkedIn/email/phone fields; no duplicate components. Full cross-surface visual QA after deploy.  
- Maritza/Alejandra live Google runs deferred (`GDI_LIVE_GOOGLE_THREE_CASE=1`) to keep spend bounded.

## 7. Certifications

```
GOOGLE_API_READY = YES
PROVIDER_ORDER = saved → google_gemini → exa → verify → surfe
GOOGLE_FIRST_OFFLINE = YES
THREE_CASE_OFFLINE = YES
ISLAS_LIVE_GOOGLE_GROUNDED = YES (channels verified; Exa secondary used)
CONSUMER_AI_MODE_PARITY = NO
FULL_AIRTABLE_APPLY = PENDING_FOUNDER_AUTH
RAILWAY_DRAWER = PENDING_IC_MERGE_DEPLOY
```
