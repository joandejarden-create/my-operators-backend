# GDI Contact Pilot Hardening V1 — Founder Report

**Status:** STOP — $0 hardening complete. Awaiting founder review / Integration Controller before next paid run.

**Branch:** `feat/gdi-contact-pilot-hardening-v1`  
**Base:** `c6272fb737fbacf0cf9916c331465659351a65b6` (#585)

---

## 1. Permanent primary/secondary uniqueness

| Change | Location |
|---|---|
| `isSameVerifiedPerson` / `enforceDistinctPrimarySecondaryContacts` | `gdi-named-contact-coverage-standard-v1.js` |
| Do not preserve duplicate secondary | `secondary-named-contact-v1.js` → `attachSecondaryNamedContact` |
| Clear duplicate on commercial projection | `gdi-commercial-contact-selection-v1.js` |
| ALT stage enforces distinctness | `gdi-buyer-alternative-person-research-v1.js` |
| Do not fabricate secondary attempts; fix `NOT_ATTEMPTED` regex false positive | coverage standard |

**Regression:** `npm run test:gdi-primary-secondary-contact-uniqueness-v1` — **7/7 PASS**

---

## 2. STAGED coverage gate — audit & resolution

| Package | coverageOk | integrityOk | Pointer changed |
|---|---|---|---|
| `gdi_dh_contact_hardened_rece0NdV9FCnaFlVS_20261124` | **true** | true | **false** |
| `gdi_dh_contact_hardened_recaokE8psIbuVeuH_20261124` | **true** | true | **false** |
| `gdi_dh_contact_hardened_rec1t35e3sPerzxYu_20261124` | **true** | true | **false** |

**Method (honest, $0):** For every named opportunity, ran saved-evidence secondary pass (`evaluateSecondaryContactOpportunity` + `attachSecondaryNamedContact`), then documented `SECOND_VERIFIED` or `ATTEMPTED_NO_SECOND_VERIFIED`. Pilot secondaries (Luísa Marques, Robert Govers, Olaf Kelber) preserved from live evidence. No fabricated attempts. Historical V3 / best-contact / buyer-who-recovery / pilot4 packages preserved unchanged.

Audit artifact: `COVERAGE_GATE_AUDIT.json`

---

## 3. Four pilot results preserved

| Opportunity | Primary | Secondary | Finalized |
|---|---|---|---|
| Rainbow Cup | Jason Gooding | — (attempted, none distinct) | yes |
| Meeting Point | Filomena Marcelo | Luísa Marques | yes |
| IPBA 2026 | João Freire | Robert Govers | yes |
| GA Society | Cornelia Kern | Olaf Kelber | yes |

Included in hardened STAGED candidates above. Active STAGED pointers **not** flipped.

---

## 4. Unseeded opportunities (next live trial)

Selected **10** commercially strong, naturally unseeded cases (no verified buyer, no direct-buyer seed, known demand org). Proposed budget **~$7.50** ($0.75/opp, flexible under a new founder GO).

| # | Hotel | Organization | Opportunity ID | Score |
|---:|---|---|---|---:|
| 1 | Condesa DF | World Association for Public Opinion Research | `disc_0ca4947f4e8e` | high |
| 2 | Condesa DF | Business History Conference | `disc_af7764724e28` | |
| 3 | Condesa DF | World Institute of Pain | `disc_effeb89206de` | |
| 4 | Condesa DF | Journey Mexico | `disc_34fbbd2cd046` | |
| 5 | LaLuna | CARICOM | `disc_f7c52d3a8036` | |
| 6 | LaLuna | Grenada Triathlon Association | `disc_4d1398578d14` | |
| 7 | LaLuna | Caribbean Telecommunications Union + Gov Grenada | `disc_fe917e4f6b78` | |
| 8 | LaLuna | CWWA with NAWASA and GSWMA | `disc_1bf10c887cb8` | |
| 9 | Memmo | Globalization and Localization Association (GALA) | `disc_39da37932d78` | |
| 10 | Memmo | LáFora | `disc_190fa2a9eafd` | |

**Pipeline:** CONTACT_RESEARCH → BUYER_ALTERNATIVE_PERSON_RESEARCH → HOW  
**Seeding:** NONE  
**Stop:** new Exa GO ceiling; per-opp exhaustion; 3× chrome streak; no Findymail/Surfe/Airtable/CURRENT/auto STAGED

---

## 5. Live ALT ladder — mocked proof ready

Exact expected trace:

1. IDEAL_BUYER_NOT_FOUND  
2. NEXT_BUYER_RESEARCH_STRATEGY  
3. ALTERNATIVE_ROLE_SEARCH  
4. PERSON_VERIFICATION  
5. COMMERCIAL_RANKING  
6. SECONDARY_CONTACT_SEARCH  
7. PUBLIC_HOW_DISCOVERY  

**Regression:** `test:gdi-buyer-alternative-person-research-v1` check **19** — CERTIFICATION_PASS (19/19)  
Artifact: `reports/gdi-buyer-alternative-person-research-v1/LIVE_ALT_PERSON_TRACE_EXPECTATIONS.json`

Do not force paid searches merely to exercise the ladder; use the unseeded list above.

---

## 6. Regression results

| Test | Result |
|---|---|
| `test:gdi-primary-secondary-contact-uniqueness-v1` | PASS (7) |
| `test:gdi-named-contact-coverage-standard-v1` | PASS (10) |
| `test:gdi-buyer-alternative-person-research-v1` | PASS (19) |

---

## 7. Governance

- $0 Exa / Findymail / Surfe  
- No Airtable writes  
- No STAGED pointer changes  
- No CURRENT / customer publication  
- Dirty registered worktrees preserved (implementation on isolated temp WT → feature branch)

**STOP** for founder review before the next paid research run.
