# Operator Setup P1 staging proof

**Date:** 2026-05-26  
**Purpose:** Prove P1 fields flow Setup → new-base Airtable → Explorer prefill → OAS `/companies` → Operator Strategy row model **before** Phase E (Deal Operator Review Set).

**Constraints honored:** No production `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1`; no Airtable schema / scoring / BAS / OCS / OAS PDF / My Deals UX changes.

---

## Environment

| Item | Value |
|------|--------|
| Base | `appvtnDurnMSjINP6` (from local `.env`) |
| Writer flag during proof | `OPERATOR_SETUP_USE_NEW_BASE_WRITER=1` **in process only** (script); `.env` unchanged |
| `OPERATOR_EXPLORER_HIDE_TEST_RECORDS` | `0` |
| Proof method | Direct new-base writer + Airtable readback + in-process OAS companies builder (same as API) |

---

## Test operator

| Field | Value |
|-------|--------|
| **Master record id** | `recBVEgtm8cS96mu7` |
| **Company name** | `P1 Staging Proof Sandbox (Do Not Use Production)` |
| **submission_status** | Patched to **Active** after save (writer defaults to `Submitted`; Explorer/OAS require **Active**) |

**Reports:**

- [operator-setup-p1-staging-save-proof-2026-05-26T21-28-47.json](../reports/operator-setup-p1-staging-save-proof-2026-05-26T21-28-47.json) — initial write + readback
- [operator-setup-p1-staging-save-proof-2026-05-26T21-29-08.json](../reports/operator-setup-p1-staging-save-proof-2026-05-26T21-29-08.json) — OAS companies for sample deal

---

## P1 payload (exact fixture options)

Saved via `writeOperatorSetupToNewBase` with values from `public/fixtures/operator-alignment-field-options.json`:

- **Admin:** Data Confidence = Operator-provided; Source Type = Operator-provided; Last Updated = run date
- **Market:** Mexico, United States; Cancún, Riviera Maya; Active operations
- **Operating:** Select-service + Focused-service; Upper Upscale + Upscale; Full third-party management
- **Services:** Full hotel management, Pre-opening planning, Revenue management, Owner reporting (Offered Services)
- **Opening:** Strong (New-Build + Pre-Opening)
- **Capabilities:** Institutional reporting; Centralized support (revenue); Regional + Global sales; Moderate F&B

**Not tested:** `companyLogo` (attachment pipeline skipped by design).

---

## Save result

| Check | Result |
|-------|--------|
| New-base writer | **Pass** — Master + 4 one-to-one tables created |
| Record ids | Master `recBVEgtm8cS96mu7`; Profile/Platform/Commercial/Governance child rows created |
| Warnings | None |
| Post-save patch | `submission_status` → **Active** (staging proof step; not automatic on writer today) |

---

## Airtable readback (18 P1 fields)

All **18** non-system P1 fields: **pass** (expected = actual in Airtable + prefill).

Tables written:

- **Master:** `company_name`, Data Confidence, Source Type, Last Updated
- **Profile & Positioning:** `companyDescription`, `primaryServiceModel`, `chainScalesSupported`, Service Models Supported (build sheet table)
- **Platform & Markets:** Active Countries, Active Markets, Market Presence Type
- **Commercial:** Management Structures, New-Build, Pre-Opening
- **Governance:** Offered Services, Owner Reporting, Revenue Management, Sales Platform, F&B Capability

---

## Operator Explorer

| Check | Result |
|-------|--------|
| Prefill keys for Phase D rail | **Pass** — 24 populated keys on readback |
| Explorer sections A–G | **Pass** (automated) — each P1 key present in `buildPrefillObjectFromNewBaseRows` output |
| Live UI / gold-mock | **Pass** (profile rail) — see [operator-setup-p1-ui-qa-before-phase-e.md](./operator-setup-p1-ui-qa-before-phase-e.md) |
| Alignment Context (`dealId`) | **Partial** (UI) — data **Pass** in-process; standalone page needs auth for panel; messaging fix in `operator-explorer-new-base-profile.js` |

---

## OAS `/companies` API

Deal used: `recIeGRZP21udmTnt` (sample deal from existing QA script).

| Check | Result |
|-------|--------|
| In active candidates | **Yes** (12 active operators loaded) |
| In `companiesForConsideration` | **Yes** (11 companies ranked) |
| Sample row | Moderate Alignment Signals; score 73; data confidence Operator-provided; alignment signals present |

---

## Operator Strategy table

| Check | Result |
|-------|--------|
| API row model | **Pass** — same `companiesForConsideration` shape Strategy uses |
| My Deals UI | **Partial** — API row model **Pass**; browser table **blocked without Memberstack** — see [operator-setup-p1-ui-qa-before-phase-e.md](./operator-setup-p1-ui-qa-before-phase-e.md) |

---

## Failures / warnings

1. **submission_status:** Writer sets `Submitted`; OAS/Explorer list require **Active**. Staging proof patches Active via Airtable REST after save. Product should automate Active publish path before production cutover.
2. **266 static-form-only fields** still not on new-base — out of P1 scope but blocks “full consistency.”
3. **companyLogo** not in proof.
4. **Browser Explorer** — profile rail verified 2026-05-26; Strategy tab needs authenticated pass.
5. **UI QA report:** [operator-setup-p1-ui-qa-before-phase-e.md](./operator-setup-p1-ui-qa-before-phase-e.md) · `node scripts/qa-operator-setup-p1-ui-before-phase-e.mjs`

---

## Scripts

```bash
# Write sandbox operator + readback report
node scripts/run-operator-setup-p1-staging-proof.mjs --write

# Re-verify existing operator (+ optional deal)
node scripts/run-operator-setup-p1-staging-proof.mjs --operator-id recBVEgtm8cS96mu7 --deal-id recIeGRZP21udmTnt

# Validation only
node scripts/validate-operator-setup-p1-staging-proof.mjs --operator-id recBVEgtm8cS96mu7 --deal-id recIeGRZP21udmTnt
```

---

## Final verdict

| Question | Answer |
|----------|--------|
| P1 saved through new-base writer in staging? | **Yes** |
| P1 in Operator Explorer (data layer)? | **Yes** (prefill + live gold-mock rail) |
| P1 in OAS `/companies`? | **Yes** (with Active status + sample deal) |
| P1 in Operator Strategy? | **Partial** (API row OK; My Deals UI needs logged-in confirmation) |
| Phase E safe to begin? | **No** — finish authenticated Operator Strategy + Alignment Context UI sign-off ([UI QA doc](./operator-setup-p1-ui-qa-before-phase-e.md)) |
| Production new-base writer safe to enable? | **No** — remain `OPERATOR_SETUP_USE_NEW_BASE_WRITER=0` in production until broader backfill + publish workflow |

---

## Regenerate

```bash
node scripts/run-operator-setup-p1-staging-proof.mjs --write --deal-id recIeGRZP21udmTnt
node scripts/validate-operator-setup-p1-staging-proof.mjs --operator-id <recId> --deal-id <dealRecId>
```
