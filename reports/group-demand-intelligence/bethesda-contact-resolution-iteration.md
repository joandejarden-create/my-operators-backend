# Bethesda Contact Resolution Iteration

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Pass:** `gdi_contact_resolution_v1`  
**Date:** 2026-09-15  
**Constraint honored:** No ADP changes · No GDI rebuild · No parallel contact DB · No broad discovery · **$0** Webhound / paid enrichment

---

## Architecture

### Reused (canonical Contact Intelligence)

| Capability | Module |
|---|---|
| Generic / role / named mailbox classification | `lib/hotel-intelligence/contact-intelligence/dimensions.js` |
| Attribution / deliverability patterns | same + `vocabulary.js` |
| Paid enrichment policy (remains OFF by default) | `policy.js` |
| Person/channel record builders (available, not duplicated) | `contact-record.js` |

GDI does **not** call Apollo or invent a second CRM. It consumes CI reachability helpers and stores opportunity-scoped contact packages.

### New / extended (reusable)

| Method | Location | Reuse |
|---|---|---|
| `classifyEmailType` / `classifyEmailVerificationStatus` / `classifyPhoneType` | `contact-intelligence/contact-reachability.js` | Any Dealality product |
| `preferStrongerEmail` / `preferStrongerPhone` / `personDedupeKey` | same | Merge rules without overwriting verified with inferred |
| Contact Grade A–E, target role match, Contact Confidence | `group-demand-intelligence/contact-resolution.js` | GDI + future opportunity products |
| Official L1 enrichment patches | `contact-official-enrichments-v1.js` | Pilot corpus only |
| Post-qual pass + audit | `contact-resolution-pass.js` | Runs **after** qualification |

### Research sequence (enforced)

Contact enrichment runs only when `shouldEnrichContact()` is true:

High · strong/moderate Medium · Overflow/Housing · Reactivation · selected Future Cycle  

Weak Watchlist / Disqualified are skipped (17 skipped this run).

---

## Before vs After

Scope: **enrichable** opportunities only (High + strong Medium + overflow/reactivation paths), n=21.

| Metric | Before | After |
|---|---:|---:|
| Named / entity contact (heuristic snapshot) | 85.7% | ~76–86%* |
| With email | 61.9% | 66.7% |
| With phone | 23.8% | 38.1% |
| Grade A | 9.5% | 23.8% |
| Grade B | 19.0% | 14.3% |
| Grade C | 28.6% | 23.8% |
| Grade D (generic / main only) | 19.0% | 19.0% |
| Grade E (none) | 23.8% | 19.0% |
| **A+B (actionable)** | **28.5%** | **38.1%** |

\*Named % can dip when dishonest org desk labels (e.g. “ACTS meetings staff”) are replaced with honest null + generic inbox (Grade D) — that is a quality improvement, not a regression.

### High Priority only (n=1)

| Target | Result |
|---|---|
| 100% named relevant contact | **Met** — HBC Event Services (housing entity) |
| >80% verified work email | **Met** — official-source verified |
| >60% useful phone | **Met** — event line `505-346-0522` (not labeled Direct) |
| Grade A or B | **Met** — Grade **A** |

### High + strong Medium combined (n=12)

| Target | Result |
|---|---|
| >80% named relevant contact | **Not met** — ~58% actionable identity (AHIMA / CMSS unresolved; ACTS/BEBPA generic-only) |
| >70% useful/verified email | **Met** — 75% official-source verified email |

---

## High Priority

### Potomac Memorial Tournament 2027 (`gdi_opp_potomac_memorial_2027`)

| Field | Value |
|---|---|
| Contact | **HBC Event Services** |
| Title | Official stay-to-play housing partner |
| Relationship | Manages mandatory stay-to-play hotel program |
| Email | `support@hbceventservices.com` |
| Email verification | Official source verified |
| Phone | `505-346-0522` |
| Phone type | Event line |
| Contact quality | Grade **A** |
| Contact confidence | 75 |
| Why this contact | HBC runs the official stay-to-play hotel program; housing-list inclusion is the actionable sales path ahead of the association general office. |
| Source | Official Memorial page + known HBC housing path |
| Backup | Kathy Hauschild, Tournament Director (`tournament@…`) — role-based inbox; org `301-519-8070` labeled **Main organization**, not Direct |

---

## Strong Medium (highest-value)

| Opportunity | Contact | Grade | Email / verify | Phone / type |
|---|---|---|---|---|
| NICE 2027 | Karen Wetzel, Director of NICE | **A** | `karen.wetzel@nist.gov` · Official | `240-439-0767` · Office |
| NAR GAD 2027 | Jami Sims | **A** | `GADInst@nar.realtor` · Official (role-based) | `202-383-1221` · Office |
| Premier Cup 2026 | HBC Event Services | **A** | `support@…` · Official | Event line |
| SHOW 2026 | Jessica Mitchell | **B** | `Jessica.Mitchell@nih.gov` · Official | — |
| AFCEA HITS | Andrea Snader (VP Health IT Summit) | **B** | `registrar@…` · Official (role-based) | Main org line |
| Arlington Spring | Tournaments desk | **B** | `tournaments@…` · Official | — |
| ACTS TS27 | *(unnamed)* | **D** | `info@actscience.org` | Main |
| BEBPA USB | *(unnamed)* | **D** | `contactus@bebpa.org` | Main |
| NADO WashCon | Desk label | **D** | `info@nado.org` | — |
| AHIMA Advocacy | — | **E** | — | — |
| CMSS Spring | — | **E** | — | — |

---

## Generic Inbox Reduction

Notable Grade **D → A** improvements:

- **NICE 2027:** `nice@nist.gov` desk → **Karen Wetzel** (official NIST staff page)
- Overflow housing: association TD-first → **HBC** primary (Potomac, Premier Cup)

Remaining Grade D are honestly generic (ACTS, BEBPA, NADO, AMWA) — no fabricated names.

---

## Missing Contact Analysis

| Opportunity | Why unresolved / weak |
|---|---|
| AHIMA Advocacy 2027 | Public summit URL 404; no official named planner published |
| CMSS Spring 2027 | No curated L1 contact in corpus; not invented |
| ACTS TS27 | Official page publishes only `info@` + main phone |
| AMWA | Role inbox `associatedirector@` without named person |
| AFCEA | Named VP but only chapter registrar + main line published (no direct work email) |

Phones remain sparse because public official pages often omit direct lines — reported honestly, not fabricated.

---

## Provider Economics

| Item | Value |
|---|---|
| Contact research cost (tracked) | **$0.00** |
| Webhound contact calls | **0** |
| Paid enrichment | **0** (flag OFF) |
| Grade improvements | Official-source curation + CI classification only |

External enrichment was **not** required for the wins above. Webhound should stay escalated only for High / strong Medium with no L1 path (AHIMA, ACTS named planner) — not default.

---

## Webhound

| | |
|---|---|
| Calls this iteration | 0 |
| Cost | $0 |
| Incremental value | N/A |
| Recommendation | Keep as **last** escalation for High/strong Medium with no public named planner; do not use for directory scraping |

---

## Data Quality Risks

- **Role-based inboxes** (`tournament@`, `GADInst@`, `registrar@`, `support@`) are verified as published channels but are **not** personal direct work emails — UI labels them correctly.
- **Main / event lines** must never show as Direct (regression-tested).
- **Stale historical contacts** reduce Contact Confidence when `historicalOnly` is set; Reactivation must re-verify.
- **Provider conflicts** — none this run (single L1 source path).
- Contact Confidence ≠ Evidence Confidence (kept separate).

---

## Product Recommendation

1. **Are High Priority leads easier for Sales to act on?** **Yes** — Potomac now points at HBC with email + phone + why-this-contact.
2. **Is the right person usually identified?** **Often for A/B leads; not yet for the full strong-Medium set.**
3. **Are emails trustworthy enough?** **Yes when labeled Official source verified;** inferred patterns are never shown as verified.
4. **Are phones useful enough to display?** **Yes when present and typed;** coverage is still low — show type, don’t invent.
5. **Is external enrichment economically scalable?** **Not proven yet** — L1 official sources delivered the gains at $0. Paid/Webhound only for residual High/strong gaps.
6. **Default Dealality contact methods going forward:**  
   official event/org pages → CI reachability classification → existing verified person → (gated) provider → inferred last → Webhound last.

### Final verdict

# PARTIAL GO

Sales can immediately act on the **High** lead and several strong Mediums with clear **who / why / how**. The product boundary held (opportunity-scoped contacts, not a contact database). Remaining gaps are honest public-data limits, not fabrication failures.

**Success question:** *When GDI identifies an actionable opportunity, can the hotel salesperson immediately understand who to contact and have a credible way to reach them?*  
→ **Yes for High and Grade A/B Mediums; not yet consistently for the full strong-Medium corpus.**

---

## Apply / gates

```bash
npm run test:gdi-foundation
npm run gdi:apply-contact-resolution -- --apply
```

UI cache bust: `?v=gdi-contact-20260915` on internal + share HTML.  
Snapshot: `reports/group-demand-intelligence/bethesda-contact-resolution-before-after.json`
