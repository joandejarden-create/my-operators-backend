# GDI Contact Identity Preflight — Founder Acceptance Report

Generated: 2026-11-20T09:23:51.610Z

## Verdict

Pilot of **20** opportunities complete offline. **No paid Surfe / Apollo / new research** in this phase.

| READY | PARTIAL | UNRESOLVED |
| ---: | ---: | ---: |
| 3 | 15 | 2 |

- Employer name coverage: **20/20**
- Employer domain present: **9/20** (verified: **9**)
- Verified personal LinkedIn: **3/20**
- Incorrect identities rejected: **1**
- Surfe inputs prepared (no calls): **20/20**
- Research spend this pilot: **$0** (reused prior evidence)

## 1. Architecture audit (summary)

- Canonical GDI contacts: hotel-scoped `canonical-contacts.json` + research inventory slim payload
- CCI / Google-first / Exa / LinkedIn classification: reused (`classifyLinkedInProfileMatch`)
- Org domain: email domain + `resolveOfficialDomains` heuristics
- Surfe: `lib/surfe/client.js` fields `firstName/lastName/companyName/companyDomain/linkedInUrl`
- New permanent component: `gdi-contact-identity-preflight-v1.js` + Surfe input adapter
- Does **not** create a competing discovery engine

## 2. Pilot opportunity IDs

- `rx_d_2400ebe4c2bb` — Abbas Zavar — **READY** (LINKEDIN_VERIFIED)
- `hist_de85f642e969` — Abir Chermiti — **READY** (LINKEDIN_VERIFIED)
- `wy_pd_fc13e2e08dd5` — Daryl Simon — **PARTIAL** (LINKEDIN_VERIFIED)
- `gdi_opp_divcult_spring_webinar_7` — Michel Lafleur — **READY** (LINKEDIN_VERIFIED_SLUG_VARIANT)
- `paid3_0a6b19883953` — Alejandra Vázquez — **PARTIAL** (LINKEDIN_MISSING_STRONG_EMAIL)
- `gdi_opp_insurance_risk_capital_bermuda_2026_0` — Adam Jordan — **PARTIAL** (LINKEDIN_MISSING_STRONG_EMAIL)
- `gdi_opp_ndss_advocacy_2027` — Amy Drow — **PARTIAL** (LINKEDIN_MISSING_ASSOCIATION)
- `gdi_opp_autoamericas_2027` — Andrés Caballero — **PARTIAL** (LINKEDIN_MISSING)
- `hist_9d12871f512d` — Abraham Yasir Maciel Montoya — **PARTIAL** (LINKEDIN_MISSING_GOV)
- `wy_pd_5cc46249ac1f` — Bacilio Sánchez — **PARTIAL** (EMPLOYER_ORGANIZER_MISMATCH)
- `disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_` — Laura Keeley — **PARTIAL** (WRONG_CHANNEL_GENERIC_MAILBOX)
- `gdi_opp_15th_annual_boutique_hotel_investment_conference_11` — Ariela Kiradjian — **PARTIAL** (LINKEDIN_MISSING_ENTRY_POINT)
- `rx_d_2d58b95c5905` — Abramo A. Chabib — **PARTIAL** (UNRESOLVED_QUARANTINED)
- `gdi_opp_iafl_usa_canadian_chapter_meeting_10` — IAFL — **UNRESOLVED** (UNRESOLVED_ORG_NOT_PERSON)
- `gdi_opp_wrome_pilot_v0_red_bull_sailgp_rome_2027` — Francesco Francavilla — **PARTIAL** (LINKEDIN_MISSING_ENTRY_POINT)
- `gdi_opp_iaadfs_summit_of_the_americas_2027_6` — Olivia Jallits — **PARTIAL** (LINKEDIN_MISSING)
- `disc_1490e4966ee8` — David Mendoza Martínez — **PARTIAL** (MULTIPLE_EMPLOYER_LABELS)
- `gdi_opp_cda_annual_meeting_2026_8` — CDA Contact — **PARTIAL** (UNVERIFIED_COMPANY_DOMAIN)
- `wy_pd_c6455b6578ee` — Dra. Carmen Encarnación — **PARTIAL** (NAME_VARIANT_PARTIAL)
- `gdi_pe_879047c3211d2e82` — CityCruises — **UNRESOLVED** (ORG_LABEL_NOT_PERSON)

## 3–7. Coverage & relevance

See `PILOT_20_RESULTS.json` metrics, incorrect rejections, and employer/organizer splits.

## 8. Airtable

DRY_RUN_NO_WRITES. Persist + read-back certification deferred until founder approves apply for READY/PARTIAL identities. Surfe-derived fields remain prohibited in canonical public-contact fields.

## 9. Surfe request preparation examples (no paid calls)

```json
[
  {
    "opportunityId": "rx_d_2400ebe4c2bb",
    "personName": "Abbas Zavar",
    "readiness": "READY",
    "surfeFieldNames": {
      "firstName": "Abbas",
      "lastName": "Zavar",
      "companyName": "University of Toronto — Institute of Health Policy, Management and Evaluation",
      "companyDomain": "ihpme.utoronto.ca",
      "linkedInUrl": "https://www.linkedin.com/in/abbaszavar"
    },
    "fieldsProvided": {
      "firstName": {
        "value": "Abbas",
        "verified": true
      },
      "lastName": {
        "value": "Zavar",
        "verified": true
      },
      "companyName": {
        "value": "University of Toronto — Institute of Health Policy, Management and Evaluation",
        "verified": true
      },
      "companyDomain": {
        "value": "ihpme.utoronto.ca",
        "verified": true
      },
      "linkedinUrl": {
        "value": "https://www.linkedin.com/in/abbaszavar",
        "verified": true,
        "status": "VERIFIED_LINKEDIN_PROFILE"
      }
    },
    "surfeCallExecuted": false
  },
  {
    "opportunityId": "hist_de85f642e969",
    "personName": "Abir Chermiti",
    "readiness": "READY",
    "surfeFieldNames": {
      "firstName": "Abir",
      "lastName": "Chermiti",
      "companyName": "IEEE",
      "companyDomain": null,
      "linkedInUrl": "https://www.linkedin.com/in/abirchermiti"
    },
    "fieldsProvided": {
      "firstName": {
        "value": "Abir",
        "verified": true
      },
      "lastName": {
        "value": "Chermiti",
        "verified": true
      },
      "companyName": {
        "value": "IEEE",
        "verified": true
      },
      "companyDomain": {
        "value": null,
        "verified": false
      },
      "linkedinUrl": {
        "value": "https://www.linkedin.com/in/abirchermiti",
        "verified": true,
        "status": "VERIFIED_LINKEDIN_PROFILE"
      }
    },
    "surfeCallExecuted": false
  },
  {
    "opportunityId": "wy_pd_fc13e2e08dd5",
    "personName": "Daryl Simon",
    "readiness": "PARTIAL",
    "surfeFieldNames": {
      "firstName": "Daryl",
      "lastName": "Simon",
      "companyName": "The Tie",
      "companyDomain": null,
      "linkedInUrl": null
    },
    "fieldsProvided": {
      "firstName": {
        "value": "Daryl",
        "verified": true
      },
      "lastName": {
        "value": "Simon",
        "verified": true
      },
      "companyName": {
        "value": "The Tie",
        "verified": true
      },
      "companyDomain": {
        "value": null,
        "verified": false
      },
      "linkedinUrl": {
        "value": null,
        "verified": false,
        "status": "CANDIDATE_LINKEDIN_PROFILE"
      }
    },
    "surfeCallExecuted": false
  },
  {
    "opportunityId": "gdi_opp_divcult_spring_webinar_7",
    "personName": "Michel Lafleur",
    "readiness": "READY",
    "surfeFieldNames": {
      "firstName": "Michel",
      "lastName": "Lafleur",
      "companyName": "Faculty of Sociology, University of A Coruña",
      "companyDomain": null,
      "linkedInUrl": "https://www.linkedin.com/in/jean-michel-lafleur-480119342"
    },
    "fieldsProvided": {
      "firstName": {
        "value": "Michel",
        "verified": true
      },
      "lastName": {
        "value": "Lafleur",
        "verified": true
      },
      "companyName": {
        "value": "Faculty of Sociology, University of A Coruña",
        "verified": true
      },
      "companyDomain": {
        "value": null,
        "verified": false
      },
      "linkedinUrl": {
        "value": "https://www.linkedin.com/in/jean-michel-lafleur-480119342",
        "verified": true,
        "status": "VERIFIED_LINKEDIN_PROFILE"
      }
    },
    "surfeCallExecuted": false
  },
  {
    "opportunityId": "paid3_0a6b19883953",
    "personName": "Alejandra Vázquez",
    "readiness": "PARTIAL",
    "surfeFieldNames": {
      "firstName": "Alejandra",
      "lastName": "Vázquez",
      "companyName": "IGECO (Italian German Exhibition Company)",
      "companyDomain": "igeco.mx",
      "linkedInUrl": null
    },
    "fieldsProvided": {
      "firstName": {
        "value": "Alejandra",
        "verified": true
      },
      "lastName": {
        "value": "Vázquez",
        "verified": true
      },
      "companyName": {
        "value": "IGECO (Italian German Exhibition Company)",
        "verified": true
      },
      "companyDomain": {
        "value": "igeco.mx",
        "verified": true
      },
      "linkedinUrl": {
        "value": null,
        "verified": false,
        "status": "NO_PROFILE_FOUND"
      }
    },
    "surfeCallExecuted": false
  },
  {
    "opportunityId": "gdi_opp_insurance_risk_capital_bermuda_2026_0",
    "personName": "Adam Jordan",
    "readiness": "PARTIAL",
    "surfeFieldNames": {
      "firstName": "Adam",
      "lastName": "Jordan",
      "companyName": "InsuranceERM",
      "companyDomain": "insuranceerm.com",
      "linkedInUrl": null
    },
    "fieldsProvided": {
      "firstName": {
        "value": "Adam",
        "verified": true
      },
      "lastName": {
        "value": "Jordan",
        "verified": true
      },
      "companyName": {
        "value": "InsuranceERM",
        "verified": true
      },
      "companyDomain": {
        "value": "insuranceerm.com",
        "verified": true
      },
      "linkedinUrl": {
        "value": null,
        "verified": false,
        "status": "NO_PROFILE_FOUND"
      }
    },
    "surfeCallExecuted": false
  }
]
```

## 10. Research spend

- This pilot: **$0**
- Reused second-pass spend (prior): $NaN
- Reused Apify pilot delta (prior): $0.0884

## 11. Integration / PR status

- PR #484: **OPEN** — https://github.com/joandejarden-create/my-operators-backend/pull/484
- Work landed on `feat/gdi-google-style-contact-search-before-surfe-v1` (same contact-enrichment stream)
- GDI must **not** merge — Integration Controller owns merge

## 12. Recommended phased rollout

- **phaseA:** Expand identity preflight offline across all eligible GDI opportunities with named contacts (no paid search).
- **phaseB:** Bounded Gemini LinkedIn discovery only for PARTIAL/UNRESOLVED with verified employer — batch ≤20, budget-capped.
- **phaseC:** Airtable idempotent persist + read-back for READY and high-confidence PARTIAL identities.
- **phaseD:** Optional controlled Surfe A/B (name+company vs +domain vs +verified LinkedIn) under explicit founder auth.

## STOP

STOP for founder review before additional paid research, full database backfill, or paid Surfe enrichment.
