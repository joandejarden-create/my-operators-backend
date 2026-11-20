# GDI — Google-style public contact search before Surfe

**Date:** 2026-11-20  
**Branch:** `feat/gdi-google-style-contact-search-before-surfe-v1`  
**Workspace:** `C:\Dev\dealality\ws-gdi`

## 1. Exact root cause

Find Email & Phone (`POST …/contacts/reveal`) called **Surfe only**. Public Contact Discovery 2.0 existed for research/pilots but was **not** on the button path.

For Francisco Javier Islas Godoy specifically:

1. No public search ran on CTA click → Surfe NO_RESULT → UI said “No email or phone found”.
2. Even when Exa was used in research, early-stop treated list `/directorio/` pages as sufficient, and **Node TLS failed** on `portal.hcg.gob.mx` (“unable to verify the first certificate”), so the official `directorioind` profile was skipped.
3. Honorific `Lic.` and surname-order noise weakened person matching; extension regex could over-capture CSS tokens.

## 2. Code changes

| Area | Change |
|------|--------|
| Query plan | Progressive Google-style strategies A–E + org-domain inference from evidence |
| Extraction | Honorific strip, surname-order keys, person-profile URL priority, MX E.164, Ext./Extensión/Anexo |
| TLS | Research-only insecure TLS **retry** after cert failure (SSRF/DNS pinning retained) |
| Resolver | `gdi-missing-channel-resolver-v1.js` — saved → public search → persist hook → Surfe last |
| Reveal API | `postGdiContactReveal` uses resolver; Surfe cannot suppress public hits |
| Drawer | Office Phone + Extension(s) + tel: link + Copy dial string |
| Gate | `test:gdi-three-case-public-contact-search-before-surfe-v1` |

## 3. Three-case results

### Offline (deterministic)

`npm run test:gdi-three-case-public-contact-search-before-surfe-v1` → **PASS**

| Contact | Email | Phone | Extensions | Surfe on public hit |
|---------|-------|-------|------------|---------------------|
| Islas Godoy | fislas@hcg.gob.mx | +52 33 3883 4400 | 51621 | **skipped** |
| Maritza Medina | maritza.medina@jalisco.gob.mx | +52 33 3819 2333 | 42333 / 47542 | n/a (fixture) |
| Alejandra Vázquez | alejandra.vazquez@igeco.mx | none | — | Find Phone only |

### Live search path (Exa + TLS retry)

Islas (no fixture seed): Exa returned `directorioind` URL; recovery → **fislas@hcg.gob.mx**, **+52 33 3883 4400**, **Ext. 51621**. Cost ≈ **$0.007** (1 query).

## 4. Airtable persistence / read-back

Public channels merge onto opportunity via existing `mergePublicChannelsOntoOpportunity` / PUBLIC_SOURCE origin. On-demand reveal returns channels immediately; durable Airtable upsert continues through the existing authorized public-contact persist scripts (no Surfe PII). Full production apply for all three cases deferred until founder authorizes write (no full-DB backfill).

## 5. Cost report

- Offline gates: $0  
- Live Islas Exa verification: ~$0.007  
- Surfe: **not called** when public channels verified  
- Serper: not configured (Exa-only mode)

## 6. Certifications

```
PUBLIC_SEARCH_BEFORE_SURFE = YES
ISLAS_DIRECTORY_RECOVERY = YES
EXTENSION_PRESERVATION = YES
THREE_CASE_OFFLINE = YES
ISLAS_LIVE_EXA_PATH = YES
FULL_AIRTABLE_APPLY_THREE_CASE = PENDING_FOUNDER_AUTH
```

## 7. Remaining limitations

- Incomplete TLS chains require research-only retry (documented).  
- Commercial relevance: Islas remains procurement entry point — not auto-promoted to hotel buyer.  
- Authenticated GDI drawer + Railway SHA verification after Integration Controller merge/deploy.  
- Maritza/Alejandra live Exa re-run optional (fixtures + prior certs already green).
