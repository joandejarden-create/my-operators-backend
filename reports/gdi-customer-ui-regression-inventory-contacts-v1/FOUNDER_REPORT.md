# GDI Customer UI Regression — Inventory Visibility, Contacts & Empty States

**Date:** 2026-11-23  
**Authorization:** FOUNDER GO — **$0** investigation + targeted remediation  
**Branch:** `feat/gdi-customer-ui-regression-inventory-contacts-v1`  
**Worktree:** isolated temp (dirty `ws-gdi` + Design Hotels pilot untouched)

---

## 1. Exact root cause — “No staged version available”

**Not a missing Cambridge inventory wipe.**

| Fact | Evidence |
|---|---|
| Cambridge HPC | `recIwaP1etgx2g9nA` |
| Local research store | **8** opportunities in `data/.../hotels/recIwaP1etgx2g9nA/opportunities.json` |
| STAGED publication | **none** |
| CURRENT publication | **none** |
| UI mode that shows the message | Founder/shell **Staged** inventory (`preview=staged`) |

When inventory mode is **Staged** (or persisted localStorage Staged), the API fail-closes with `STAGED_PUBLICATION_PREVIEW_UNAVAILABLE` and refuses to fall back to research. That protection is correct.

**Defect:** customer-facing copy leaked internal staging jargon (“No staged version available for this hotel”).

**Fix:** customer-facing empty state — *“No opportunities are available for this property in the selected inventory view…”* + **View Research**. Research mode still loads the 8 Cambridge rows (`live=1`).

---

## 2. Cambridge Beaches inventory status

| Surface | Count | Status |
|---|---:|---|
| Research store | 8 | Present |
| Staged package | 0 | Legitimate empty |
| Published package | 0 | Legitimate empty |
| Hotel / Competitive (research) | 8 / 0 | Observed in UI |

**Previously authorized customer publication did not disappear** — Cambridge never had STAGED/CURRENT packages in this checkout. Research inventory remains intact and visible under Research.

---

## 3. Contact-name regression

### Root cause (UI suppression, not data loss)

`buildCanonicalCustomerPresentationContract` used `pick(preview.name, preview.displayName, tileContactName, primaryContact.name)`.

When `tileContactPreview.kind === "none"`, `displayName` / `tileContactName` are the gap label **“Contact Research Required”**, which **outranked** a real `primaryContact.name`. The card renderer then treated that gap string as the contact name (or short-circuited to research-required), hiding verified people even when email/phone were absent.

Classification of sampled losses: **A — still exists but hidden by UI logic** (plus honest **E** where no named person exists).

### Sample visibility (after fix)

| Hotel | Source | Opps | Named on tile | Contact Research Required | Named with no email |
|---|---|---:|---:|---:|---:|
| Bethesda | bundled CURRENT package | 67 | **67** | 0 | 33 |
| Bethesda | research store | 86 | 72 | 14 | 28 |
| Wyndham | research store | 82 | 23 | 59 | 6 |
| Cambridge | research store | 8 | 2 | 6 | 0 |

Browser (Bethesda published): Diane Robertson, Kathy Hauschild, Brad Roos, Lorna Kipfer, etc. visible — including cards without personal email.

Sales Ready rules **unchanged**. Missing email ≠ unknown identity.

---

## 4. Duplicates & malformed placeholders

| Finding | Assessment |
|---|---|
| Cambridge “Insurance Risk & Capital Bermuda 2026” ×2 | **Distinct opportunity IDs** with different named contacts (sponsorship vs partnership) — not render duplication |
| Bethesda local store duplicate titles | Present in research store (e.g. NCI / CTN); package CURRENT has unique IDs |
| Card list dedupe | Added by stable `id` (first wins); does not collapse distinct IDs |
| `(: )` / `(. )` placeholders | Scrubbed in titles, org meta, and tile summary prose |

---

## 5. Exact defects fixed

1. Customer empty-state copy (no “staged version” jargon) — `app.js` + API `founderMessage`
2. Canonical contact name resolution — never let gap labels overwrite `primaryContact.name`
3. Card contact renderer — reject gap labels as named people; fall through to primaryContact
4. Tile slot gate — do not hide person names solely because `customerFacingNamed === false` / missing channels
5. Malformed empty parenthetical scrub
6. Opportunity card id dedupe in filter pipeline
7. Regression tests + cache-bust

**Not changed:** Hotel / Competitive Market Scope logic; Sales Ready gates; publication authorization; paid providers.

---

## 6. Screenshots

| File | What |
|---|---|
| `01-cambridge-staged-empty-after.png` | Staged empty — customer copy + View Research |
| `02-cambridge-research-inventory-after.png` | Research — 8 opps restored; named contacts where present |
| `03-bethesda-published-named-contacts-after.png` | Published — named contacts visible without requiring email |

---

## 7. Regression tests

```
npm run test:gdi-customer-ui-regression-inventory-contacts-v1  → PASS
npm run test:gdi-missing-inventory-empty-state-v1              → PASS
npm run test:gdi-founder-preview-missing-staged-nav-v1         → PASS
npm run test:gdi-opportunity-card-quality-v1                   → PASS
npm run dealality:ci-precheck                                  → PASS (gitleaks)
```

---

## 8. PR / CI / governance

- Focused PR opened for Integration Controller (see PR URL in completion note).
- No merge / deploy from GDI chat.
- No paid research, Airtable writes, publication changes, Surfe, or Design Hotels pilot interference.
- Dirty `ws-gdi` preserved.

---

## 9. Remaining blockers / notes

1. Cambridge has **no** authorized publication package — Staged/Published stay empty by design until a separate publication GO.
2. Near-duplicate research rows with different IDs (Cambridge Insurance Risk pair) still appear — needs research reconciliation, not UI id-dedupe.
3. Live “before” screenshots of the old jargon were not captured from production; root cause proven via code + API + after screenshots.
4. Mayflower local research store is empty in this checkout (0 opps) — not part of this regression fix.

---

## Final status

**AWAITING_INTEGRATION_CONTROLLER**

**STOP** for founder review before deployment.
