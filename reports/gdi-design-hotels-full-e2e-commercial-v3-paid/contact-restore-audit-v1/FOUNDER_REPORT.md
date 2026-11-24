# GDI — Restore Best Suggested Contact & Clean Founder Staging UI

**Status:** READY FOR FOUNDER REVIEW — $0 certification complete  
**Date:** 2026-11-24  
**Branch:** `fix/gdi-best-suggested-contact-restore-v1`  
**Serving environment:** `C:\Dev\dealality\wt-ic-pr568-merge` @ `:8080`  
**Spend:** $0 (no Exa / Findymail / Surfe / Airtable / CURRENT / deploy)

---

## A. Root cause and introducing commits

### What broke

1. **Inflated STAGED finalization via generic org routes**  
   Local V3 staging (`scripts/gdi-design-hotels-full-e2e-local-stage-v3.mjs`) treated *any* HTTPS source URL as an “Official {org} organizing / lodging channel,” satisfying `contactOk` even without a named person.  
   Materialize (`gdi-design-hotels-v3-deep-materialize.mjs`) wrote the same label when a named buyer was absent.  
   Result: **128 STAGED / 15 named / 113 generic routes**.

2. **Customer-facing research jargon in STAGED empty / tile states**  
   Prior recovery work (`6a5822d15`, `80be5b88d`) restored names but left residual paths that could surface “Contact Research Required,” “Switch to Research,” and “View Research” in the founder STAGED review surface.

3. **Named-buyer yield bottleneck (pipeline, not UI alone)**  
   Paid three-hotel CohortExecute produced mostly org-route fallbacks because Exa person extraction + validation rejected many candidates as junk, while finalization accepted homepage/org URL as “complete.” Hiding labels without fixing the gate would leave intelligence broken.

### Historical contract (restored)

Opportunity tiles/drawers should show the **best verified suggested person** (name · title · organization), with relevance, source, channels, and recommended action in the drawer — not internal gap labels and not a fabricated org-route as contact completion.

---

## B. Contact quality audit — all 128 STAGED records

Audit artifact: `reports/.../contact-restore-audit-v1/SUMMARY.json` (+ per-hotel JSON + `ALL_ROWS.json`).

| Fate | Count | Meaning |
|------|------:|---------|
| **A** Found and correctly selected | **15** | Named person on STAGED |
| **C** Found but wrongly rejected / junk-shaped | **22** | Candidate strings failed person validation |
| **D** Available in prior research, not reused | **1** | Prior pilot signal not carried into V3 STAGED |
| **E** Generic route used as completion | **87** | HTTPS/org label treated as contact complete |
| **F** Genuinely unavailable (valid route only) | **3** | No named person; functional route candidate only |
| **B** Found but lost in transform | **0** | Not observed in this portfolio |

**Portfolio:** 128 staged · 15 named · 113 organization-route inflated.

---

## C. Named contacts recovered from existing evidence

Strict named restore (no new paid research): **15** people retained through restage.

| Hotel | Named (restored STAGED) |
|-------|-------------------------|
| Condesa DF | Daniel Roy; Andrew Holmes |
| LaLuna | Peter Wagner; Maurice Saad |
| Memmo Príncipe Real | Cristina Granja; Maria João Oliveira; Cátia Sousa Mano; Joana Carvalho; Paulo Pereira; Evgenii Podmogov; Lethicia Lobato Rodrigues; Robert Lück; Juanjo Suarez; Catarina Figueiredo Gonçalves; Oliver Mitwali |

Recoverable named sample from evidence: **16** (includes 1 prior-not-reused org-shaped prior for LaLuna Pure Grenada / World Sailing — not promoted as a person).

---

## D. Best-contact selection corrections

- New gate: `lib/group-demand-intelligence/presentation/gdi-verified-commercial-route-v1.js`
  - Named plausible person → finalization complete (email **not** required).
  - Gap labels (`Contact Research Required`, `Buyer Not Yet Identified`, …) **rejected** as names.
  - Org route only if verified functional destination (path evidences contact/events/lodging/etc.; homepage rejected).
- Restage: `scripts/gdi-design-hotels-best-contact-restage-v1.mjs`
  - Writes **versioned** packages `gdi_dh_best_contact_restore_*_20261124`.
  - Points STAGED at restore packages.
  - **Preserves** prior `gdi_dh_v3_staged_*` snapshots on disk (never overwritten).
- V3 stage + materialize scripts hardened to call the same gate (prevents re-inflation).

---

## E. Organization-route validity findings

| Outcome | Count |
|---------|------:|
| Generic / homepage / non-functional routes rejected under new gate | **87+** of prior org-route completions |
| Potentially valid functional routes (audit `validRouteFinalizedReady`) | **19** total vs **15** strict-named |
| Strict founder restore policy for this cycle | **Named-only STAGED** (15) |

Valid department/contact-path routes remain recognizable by the gate for future use; they were **not** auto-promoted into this certified restore package (named-person bar for Design Hotels restore).

---

## F. Genuine finalized-ready counts after stricter revalidation

| Hotel | Prior inflated STAGED | Strict named restore |
|-------|----------------------:|---------------------:|
| Condesa DF | 44 | **2** |
| LaLuna | 41 | **2** |
| Memmo | 43 | **11** |
| **Total** | **128** | **15** |

Historical 128 packages remain on disk under prior `gdi_dh_v3_staged_*` versions.

---

## G. Contact completeness by hotel (post-restore serving)

Live API `preview=staged` on `:8080`:

| Hotel ID | Count | Sample contact | Prior v3 preserved |
|----------|------:|----------------|--------------------|
| `rece0NdV9FCnaFlVS` | 2 | Daniel Roy | yes |
| `recaokE8psIbuVeuH` | 2 | Peter Wagner | yes |
| `rec1t35e3sPerzxYu` | 11 | Cristina Granja | yes |
| Bethesda `recLuxvwwxID7U2B8` | **67** (unchanged) | Sabrina Bracken | n/a |

---

## H. UI before / after (browser `:8080`)

Screenshots under serving  
`reports/gdi-design-hotels-full-e2e-commercial-v3-paid/contact-restore-audit-v1/screenshots/`:

| File | Observation |
|------|-------------|
| `01-condesa-named-restore.png` | 2/2 tiles; Andrew Holmes + Daniel Roy; no Contact Research Required |
| `02-laluna-named-restore.png` | 2/2; Peter Wagner + Maurice Saad |
| `03-memmo-named-restore.png` | 11/11 named footers |
| `04-cambridge-neutral-empty.png` | “No finalized opportunities are available for this property.” — no View Research / Switch to Research |
| `05-bethesda-67-regression.png` | 67 opportunities; named contacts intact |

**UI code:**

- Tile footer: blank when no verified person (does not emit “Contact Research Required”).
- STAGED empty: neutral copy only; optional “View Published” only when CURRENT exists — never “View Research.”

---

## I. E2E identity / deduplication findings

- CohortExecute issued **new** deep opportunity IDs (`LIVE_NEW` / `disc_*`) rather than deepening the prior ~188 discovery IDs.
- Materialize **appended** new IDs (did not delete historical inventory).
- Contact continuity: named people on chain rows merged by opportunity id when present; **1** prior-pilot signal not reused (D).
- Inflated finalized counts came from org-route gate, not from ID churn alone — but new IDs limited person-record reuse across research waves.
- No silent deletion of inventories; restore packages are additive versions.

---

## J. Leisure inclusion readiness

- Paid V3 run used **`-DisableLeisure`** intentionally to keep Exa spend inside the fourteen-segment / $18-per-hotel commercial plan.
- LDI live E2E integration **#581** is merged (`6d1cdf32f`).
- Future authorized Full E2E runs **can** include leisure by omitting `-DisableLeisure` / enabling the canonical LDI path.
- **No paid leisure research executed in this task.**

---

## K. PR and CI status

- Local certification: `npm run test:gdi-best-suggested-contact-restore-v1` → **CERTIFICATION_PASS** (15/15).
- PR: opened from `fix/gdi-best-suggested-contact-restore-v1` for Integration Controller merge (GDI chat does **not** merge).
- No CURRENT pointer change; no customer publication; Bethesda packages not targeted by restage.

---

## L. Remaining contact research (requires separate paid authorization)

- **113** opportunities remain contact-incomplete under the restored bar (excluded from certified STAGED).
- Raising named yield requires authorized **paid Exa** (and optionally later Findymail/Surfe — still HOLD) with query/validation fixes favoring real buyers over org-route volume.
- Recommended next founder decision: authorize a focused **named-buyer recovery** wave on the excluded 113 using existing evidence + targeted person search — not another volume-first discovery expansion.

---

## Governance checklist

| Constraint | Status |
|------------|--------|
| $0 only | PASS |
| No Findymail / Surfe | PASS |
| No Airtable opportunity/contact writes | PASS |
| No production publication / deploy / CURRENT auto-change | PASS |
| Historical 128 v3 packages preserved | PASS |
| Versioned restore STAGED only | PASS |
| Dirty worktrees preserved | PASS |
| STOP before further paid research / publication | **STOP** |

### Final acceptance (local)

The local GDI staging environment again displays the **best verified suggested contact person** for finalized opportunities; internal research terminology is removed from normal STAGED presentation; missing buyers are not disguised as completed contact research.

**AWAITING_INTEGRATION_CONTROLLER**
