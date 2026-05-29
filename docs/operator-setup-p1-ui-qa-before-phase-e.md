# Operator Setup P1 — UI QA before Phase E

**Date:** 2026-05-26  
**Scope:** Manual browser QA + automated data/API helper (`scripts/qa-operator-setup-p1-ui-before-phase-e.mjs`)  
**Test operator:** `recBVEgtm8cS96mu7` — *P1 Staging Proof Sandbox (Do Not Use Production)*  
**Test deal:** `recIeGRZP21udmTnt`  
**Constraints:** No Airtable schema, scoring weights, BAS/OCS/OAS PDF layout, or production `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` changes. Phase E **not** started.

---

## Summary verdict

| Area | Result | Notes |
|------|--------|--------|
| Operator Explorer UI | **Pass** (profile rail) / **Partial** (alignment + brand section) | Live new-base data; not mock |
| OAS `/companies` (data layer) | **Pass** | In-process builder matches staging proof |
| OAS `/companies` (HTTP, unauthenticated) | **Blocked** | `401 authentication_required` |
| Operator Strategy UI | **Blocked** (browser) / **Pass** (row model) | Requires Memberstack session |
| CTA wiring (code) | **Pass** | URLs match expected deal/operator ids |
| Phase E readiness | **Not yet** | Complete **authenticated** My Deals + alignment panel pass |

**Overall QA:** **Partial pass** — P1 pipeline and Explorer profile rail are verified; full UI sign-off needs one logged-in session on My Deals.

**Artifacts:**

- JSON: [reports/operator-setup-p1-ui-qa-2026-05-26.json](../reports/operator-setup-p1-ui-qa-2026-05-26.json)
- Screenshot (Explorer): `%LOCALAPPDATA%\Temp\cursor\screenshots\p1-explorer-qa.png` (local QA capture)

---

## 1. Operator Explorer UI

**URL:** `/operator-explorer-gold-mock.html?id=recBVEgtm8cS96mu7&embed=1&dealId=recIeGRZP21udmTnt`

| Check | Result |
|-------|--------|
| Company title | **Pass** — *P1 Staging Proof Sandbox (Do Not Use Production)* |
| Not mock operator | **Pass** — no demo banner; name ≠ Sample Operator Platform |
| Phase D rail sections | See table below |
| Values from new-base P1 | **Pass** — Mexico, Cancún / Riviera Maya, Select-service, Upper Upscale, Strong pre-opening, etc. |
| `GET /api/intake/third-party-operators/recBVEgtm8cS96mu7` | **200** (public read for profile) |
| Explorer list includes sandbox | **Pass** — `activeOnly=1` list includes operator id |

### Phase D rail (snapshot column)

| Section | Result |
|---------|--------|
| Profile Snapshot | **Pass** |
| Market Presence | **Pass** |
| Operating Profile | **Pass** |
| Services & Platform | **Pass** |
| Opening / Transition Support | **Pass** |
| Owner Reporting & Governance | **Pass** (governance cadence not in P1 fixture — owner reporting + source type shown) |
| Brand / Portfolio Experience | **N/A** — P1 fixture did not populate brand portfolio fields; section omitted when empty (expected) |
| Alignment Context | **Partial** — panel shows auth guidance when OAS companies fetch returns 401 (see §4) |

### Small UI fix applied (non-invasive)

`public/js/operator-explorer-new-base-profile.js` — distinguish `authentication_required` from “operator not found” so Alignment Context copy is accurate on standalone Explorer URLs without a Memberstack Bearer token.

---

## 2. OAS `/companies` API

**Endpoint:** `GET /api/operator-alignment-snapshot/recIeGRZP21udmTnt/companies`

| Check | Result |
|-------|--------|
| Sandbox in `companiesForConsideration` | **Yes** (in-process / same builder as API) |
| Score | **73** |
| Alignment band | **Moderate Alignment Signals** |
| Data confidence | **Operator-provided** |
| Review status | **May merit review based on available Operator Setup data** |
| Key consideration | **Confirm service platform depth for must-have services.** |
| Signals / validation | **Present** — Mexico markets, management structure, pre-opening samples |

**HTTP (localhost, no Bearer):** `401` + `authentication_required` — expected for protected My Deals API surface.

**Automated re-check:**

```bash
node scripts/qa-operator-setup-p1-ui-before-phase-e.mjs --base-url http://localhost:8080
```

---

## 3. Operator Strategy table (My Deals)

**URL:** `/my-deals.html?tab=operator-strategy` (with deal filter `recIeGRZP21udmTnt` when linked from deal)

| Check | Result |
|-------|--------|
| Search “P1 Staging Proof” | **Not verified in browser** — 0 rows without Memberstack auth / deals not loaded |
| Row model (API simulation) | **Pass** — same company row as OAS for deal *Aeropuerto Cancún Select-Service Hotel* |
| Column: Alignment Signal | **N/A as column** — product uses **Alignment Signal** as a **filter dropdown** only |
| Columns present in UI | Project / Deal, Operating Company, Project Location, Score, Review Status, Key Consideration, Data Confidence, CTA |

### Simulated row (authenticated session should match)

| Field | Expected |
|-------|----------|
| Operating Company | P1 Staging Proof Sandbox (Do Not Use Production) |
| Score | 73 |
| Review Status | May merit review based on available Operator Setup data |
| Key Consideration | Confirm service platform depth for must-have services. |
| Data Confidence | Operator-provided |

### CTA expectations (from `operator-strategy-my-deals.js`)

| CTA | Expected behavior | Code review |
|-----|-------------------|-------------|
| View Operator Alignment Snapshot | Opens OAS for `recIeGRZP21udmTnt` | **Pass** (`view-oas`) |
| View Operator Capability Snapshot | Opens OCS for `recIeGRZP21udmTnt` | **Pass** (`view-ocs`) |
| Open Operator Profile | `operator-explorer-gold-mock.html?id=recBVEgtm8cS96mu7&dealId=recIeGRZP21udmTnt` | **Pass** (`open-profile`) |
| Add to Operator Review | Disabled (Phase E) | **Pass** |
| Prepare Outreach | Disabled | **Pass** |

**Manual sign-off:** Log in → My Deals → Operator Strategy → search *P1 Staging Proof* → confirm row + click each CTA once.

---

## 4. Submission status behavior (document only — no change)

| Behavior | Current state |
|----------|----------------|
| New-base writer on save | Sets `submission_status` = **Submitted** |
| Explorer list + OAS candidates | Include **Active** operators only |
| P1 staging proof script | Patches **Active** after save via Airtable REST |

### Production workflow recommendation

**Option C (recommended):** Staging/demo scripts may patch **Active** for proof; production must **not** auto-activate on submit. Operators remain **Submitted** until an admin publish/approval step sets **Active** before Explorer/OAS visibility.

Options A and B remain valid product choices:

- **Option A:** Submissions stay **Submitted** until admin approval (same as C, explicit policy).
- **Option B:** Admin manually sets **Active** before Explorer/OAS (operational process).

No code change in this QA pass.

---

## 5. Issues found

| # | Severity | Issue | Action |
|---|----------|-------|--------|
| 1 | Low | Alignment Context requires auth on standalone Explorer URL | Fixed messaging; full data when opened from authenticated My Deals still needs one manual pass |
| 2 | Info | Brand / Portfolio Experience empty for P1 sandbox | Expected — brand fields not in P1 fixture |
| 3 | Blocker for UI sign-off | My Deals Operator Strategy empty without login | Re-test with Memberstack session |
| 4 | Info | OAS HTTP returns 401 without Bearer | By design — not a regression |

---

## 6. Phase E readiness

| Question | Answer |
|----------|--------|
| P1 visible in Explorer profile rail? | **Yes** |
| P1 reflected in OAS companies data? | **Yes** (Active operator + deal context) |
| Operator Strategy row provably renderable? | **Yes** (API row model) |
| Operator Strategy UI confirmed? | **No** — pending authenticated QA |
| Safe to **start Phase E implementation** now? | **No** — finish authenticated Strategy + embedded Explorer alignment check first |
| Production new-base writer? | **Still off** |

---

## 7. Constraints confirmation

- Airtable schema: **unchanged**
- Scoring weights: **unchanged**
- BAS / OCS / OAS PDF layout: **unchanged**
- `OPERATOR_SETUP_USE_NEW_BASE_WRITER` in production: **still 0**
- Phase E: **not started**

---

## Regenerate

```bash
node scripts/qa-operator-setup-p1-ui-before-phase-e.mjs --base-url http://localhost:8080
node scripts/validate-operator-setup-p1-staging-proof.mjs --operator-id recBVEgtm8cS96mu7 --deal-id recIeGRZP21udmTnt
```
