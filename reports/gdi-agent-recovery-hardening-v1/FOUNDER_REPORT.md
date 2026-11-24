# GDI Agent Recovery Hardening — Founder Report

**Phase:** $0 implementation + certification only  
**Branch:** `feat/gdi-agent-recovery-hardening-v1`  
**Base tip:** `b554a08aa4bbf4ea95ae029e962e6de54a2c0c08` (PR #587)  
**Reference live wave:** `reports/gdi-exa-agent-multi-opportunity-live-validation-v1/FOUNDER_REPORT.md`  
**New paid calls:** **0**  
**Agent default:** **OFF**  
**STAGED pointer / CURRENT / publication / Airtable:** **none**

**STOP** for founder review. Integration Controller owns merge. Do not activate the $3 scale wave in this task.

---

## 1. HOW classification — root cause and fix

### Root cause

`contacto@amegendoscopia.org.mx` (AMEG shared inbox) was labeled `DIRECT_WORK_EMAIL` / `VERIFIED_DIRECT` because:

1. `ROLE_MAILBOX_RE` and `GENERIC_ORG_EMAIL_LOCAL_RE` included English `contact` but **not** Spanish/Portuguese `contacto` / `contato`.
2. There was no person-local-part match gate: any non-listed mailbox with a source URL could become a “direct” personal email when attached to a named primary.

Company LinkedIn `/posts/` URLs could also satisfy a loose LinkedIn HOW check (`linkedin.com/` anywhere).

### Fix (permanent)

| Change | File |
|---|---|
| Expand shared-inbox locals (`contacto`, `contato`, `eventos`, `reservas`, …) | `scaling-companies-sales-presentation-v1.js`, `gdi-public-contact-verification-v1.js` |
| Require email local-part ↔ person-name token overlap for `DIRECT_WORK_EMAIL` | `emailLocalPartMatchesPerson` |
| Shared / non-matching locals → `ORGANIZATION_EMAIL_ROUTE` only | `classifyPublicContactChannel` |
| Switchboard / main-line phones → `ORGANIZATION_PHONE_ROUTE` | phone branch |
| Person LinkedIn = `/in/{slug}` only | `isPersonLinkedInProfileUrl` + WHO/HOW `hasLinkedIn` |

**Sales Ready rules were not relaxed.** `salesReady` remains an independent explicit disposition; org routes never set `emailDirect` or auto-Sales-Ready.

---

## 2. Contact / channel verification matrix (seven recoveries)

Revalidated from saved Agent traces + free public fetches only.

| Opportunity | Primary | Class | Secondary | Email ownership | Person LinkedIn | WHO | Finalize | Sales Ready |
|---|---|---|---|---|---|---|---|---|
| SPM27 | Mariano E. Navarro Arroyo | VIABLE_ALT | Luis Acebal | none | — | Y | Y | N |
| SCRM-2027 | Sai Varma Namburi | **DIRECT_BUYER** | Prakash Bandikinda | none | `/in/sai-varma-namburi-…` | Y | Y | N |
| AMEG Ecos | Mayra Membrillo Garduño | VIABLE_ALT | Armando Hernández Cendejas | **SHARED_ORG_INBOX** `contacto@…` | `/in/mayra-membrillo-…` | Y | Y | N |
| Mexico Business Summit | Jeroen Posma | VIABLE_ALT | Alma González | none | — | Y | Y | N |
| Advantage Travel | Pelin Dede | **DIRECT_BUYER** | Sonia Michaels | none | — | Y | Y | N |
| AFCONA Sales | Giovanni Conca | VIABLE_ALT | — | none | `/in/giovanni-conca-…` | Y | Y | N |
| IMRT Grenada | Larry Lawrence | VIABLE_ALT | Willvorn Grainger | **PERSON_EMAIL** `llawrence@grenadaco-opbank.com` | — | Y | Y | N |

### Focus reviews

| Person | Finding |
|---|---|
| **Pelin Dede** | Retained DIRECT_BUYER — Head of Supplier Partnerships, Lodging. Affiliation alone was not the basis; lodging partnership title is commercial pathway evidence. WHO complete without email. |
| **Sai Varma Namburi** | Retained DIRECT_BUYER — Conference Manager / conference operations. Person LinkedIn preserved as HOW channel. |
| **Larry Lawrence** | Retained VIABLE_ALT (MD / bank host pathway, not evidenced hotel buyer). Person-specific email correctly `DIRECT_WORK_EMAIL`. |

Named people preserved where direct email is unavailable. Affiliation alone does not promote to DIRECT_BUYER.

Artifact: `CONTACT_CHANNEL_MATRIX.json`

---

## 3. Updated Sales Ready assessment

| Metric | Count |
|---|---:|
| WHO_COMPLETE | 7/7 |
| Finalization-eligible (named coverage) | 7/7 |
| Person-specific professional email | 1 |
| Shared organizational inbox (correctly typed) | 1 |
| Person LinkedIn `/in/` | 3 |
| **Sales Ready** | **0/7** |

Sales Ready remains independent and unmet without an explicit sales-ready disposition plus sufficient direct outreach channels under existing product rules. Org inbox ≠ Sales Ready.

---

## 4. STAGED candidate integrity

Local packages: `staged-candidates/*.json` (7).

| Gate | Result |
|---|---|
| Demand eligibility | PASS |
| Named primary verification | PASS |
| Commercial relevance | PASS (2 DIRECT_BUYER / 5 VIABLE_ALT retained) |
| Secondary uniqueness | PASS (6 distinct; AFCONA none) |
| HOW channel verification (typed) | PASS |
| Package integrity / coverage | PASS |
| Pointer flip | **Not performed** |
| CURRENT / customer publish | **Not performed** |

---

## 5. Permanent regression results

| Suite | Result |
|---|---|
| `test:gdi-agent-recovery-hardening-v1` | **8/8 PASS** |
| `test:gdi-public-source-contact-discovery-permanent-v1` | **15/15 PASS** (includes contacto@ + Sales Ready independence) |
| `test:gdi-who-how-contact-intelligence-restore-v1` | **CERTIFICATION_PASS** |
| `test:gdi-exa-agent-contact-escalation-v1` | **25/25 PASS** |

CI wire: `.github/workflows/gdi-regression-recovery-baseline.yml` now runs public-source + hardening suites beside WHO/HOW/Agent escalation.

---

## 6. PR and CI status

- Feature branch: `feat/gdi-agent-recovery-hardening-v1`
- PR: **https://github.com/joandejarden-create/my-operators-backend/pull/588** (`OPEN`)
- Local prechecks: `dealality:ci-precheck` PASS; hardening / public-source / WHO-HOW / Agent escalation suites PASS
- GitHub CI: pending on PR
- Merge: **not performed** (IC owns)
- Registered `ws-gdi` remains on unrelated dirty branch — preserved; implementation shipped from tip-aligned worktree at `b554a08`

---

## 7. Next-wave allowlist (~30)

Prepared **without spend** from DH V3 unresolved inventory, excluding the seven recovered IDs + GALA/BHC + prior exclusions.

Artifact: `NEXT_WAVE_ALLOWLIST.json` (**30** candidates)

Examples (HIGH / future-year markers): WAPOR 2026, Nestlé Convención 2026, Akamai Partner Summit 2026, IBTM Americas 2026, … across Condesa / Memmo / LaLuna.

Each row includes hotel, country, opportunity ID, demand evidence tag, commercial priority, and `agentEligibility: PENDING_$0_PHASE1_DEMAND_VALIDATION_BEFORE_PAID_WAVE`.

**Do not treat this list as paid-ready until a fresh $0 Phase-1 demand validation pass.**

---

## 8. Proposed $3 Agent execution plan (not activated)

| Control | Proposal |
|---|---|
| Budget | **USD $3.00** new Exa Agent only |
| Effort | medium · one run / opportunity |
| Capacity | up to ~30 opps at ~$0.10 |
| Path | Canonical CONTACT → ALT(9) → conditional Agent → verify → HOW |
| Auth | Dual auth + allowlist + ceilings · Agent OFF by default |
| Preconditions | $0 demand/fit eligibility on allowlist; no seed names |
| HOW | Public-first; separate founder auth for Surfe/Findymail if needed |
| Activate now | **No** |

---

## 9. Governance confirmation

| Constraint | Status |
|---|---|
| $0 only / no Exa Agent spend | Confirmed |
| No Findymail / Surfe | Confirmed |
| No Airtable writes | Confirmed |
| No STAGED pointer flips | Confirmed |
| No CURRENT / customer publication | Confirmed |
| Existing worktrees / historical packages preserved | Confirmed |
| Agent OFF by default | Confirmed |

---

## Artifacts

| Path | Contents |
|---|---|
| `FOUNDER_REPORT.md` | This report |
| `CONTACT_CHANNEL_MATRIX.json` | Seven-opp verification matrix |
| `CERTIFICATION_SUMMARY.json` | Aggregate hardening summary |
| `staged-candidates/*.json` | Local STAGED packages |
| `how-reassessment/*.json` | Per-opp HOW reassessments |
| `NEXT_WAVE_ALLOWLIST.json` | Proposed 30-opp $3 wave (inactive) |
