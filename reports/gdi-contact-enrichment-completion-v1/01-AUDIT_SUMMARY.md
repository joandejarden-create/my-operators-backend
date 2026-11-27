# Wyndham San Diego Bayside — contact enrichment completion audit

**Hotel:** Wyndham San Diego Bayside (`recnPB3Q02NO8Ma1o`)  
**STAGED publication:** `gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261127_6e217b8a`  
**Generated:** 2026-11-27T20:37:03.743Z

## Coverage (vs hydration baseline)

| Metric | Before (baseline) | Now |
|---:|---:|---:|
| Opportunities | 26 | 26 |
| Primary named | 26 | 26 |
| Primary email | 11 | 11 |
| Primary phone | 7 | 7 |
| Primary LinkedIn | 14 | 14 |
| Unique people | 40 | 40 |
| Stage 4 annotated | 0 | 26 |

## Gap counts

### Opportunity grain (primary contact)

- Email gaps: **15**
- Phone gaps: **19**
- LinkedIn gaps: **12**

### Opportunity grain (primary + secondary named contacts)

- Email gaps: **29**
- Phone gaps: **33**
- LinkedIn gaps: **12**

### Unique person grain (40 people)

- Email gaps: **29**
- Phone gaps: **33**
- LinkedIn gaps: **12**

## Identity resolution

- **Lily Y.** (TRUNCATED_SURNAME) — disc_0fa36c29506f
- **Suzanne K.** (TRUNCATED_SURNAME) — disc_ecba7545037a
- **Alejandra (Alex) Gutierrez, PhD** (NOT_PERSON_SHAPE) — disc_67a7c2b44502

## Findymail (read-only policy)

- **Client module:** NO_DEDICATED_FINDYMAIL_CLIENT_IN_REPO — eligibility/design only; Surfe client at lib/surfe/client.js; Findymail gated in lib/group-demand-intelligence/workflow/gdi-contact-research-stage-v1.js
- **Auth:** `FINDYMAIL_API_KEY (documented in reports/hotel-explorer/HE-FINDYMAIL-IMPLEMENTATION-AUDIT-V1.md; not in .env.example)`
- **Persistence:** **not allowed by default** (persistProviderPiiByDefault: false — provider email/phone not auto-persisted to STAGED/Airtable without founder authorization)
- **Eligible primaries (current package):** 0


## Recommended next actions (no spend)

1. **Run Stage 4 public HOW (Exa/public directories) for email/phone gaps** — 0 opportunities still marked NOT_YET_RESEARCHED on at least one channel _(Align with reports/gdi-contact-research-hydration-v1/05-TARGETED_CONTACT_RESEARCH_MANIFEST.json — no Findymail/Surfe auto.)_
1. **Resolve incomplete person identities before enrichment** — Lily Y., Suzanne K., Alejandra (Alex) Gutierrez, PhD
1. **Founder-authorized Findymail batch (if pursued)** — 0 primary contacts meet strict eligibility (named+org, no usable route, no LinkedIn) _(Queue from prior restore (2 rows) may be stale vs current STAGED tip — reconcile against 00-CONTACT_AUDIT.json findymail.eligiblePrimaryContactsNow.)_
1. **Verify list vs package LinkedIn mapping** — Any EXISTS_NOT_DISPLAYED linkedin gaps

Full per-opportunity and per-person inventory: `00-CONTACT_AUDIT.json`.
