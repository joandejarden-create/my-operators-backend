# GDI — Google AI discovery parity (diagnostic + fix)

**Date:** 2026-11-20  
**PR:** [#484](https://github.com/joandejarden-create/my-operators-backend/pull/484)  
**Branch:** `feat/gdi-google-style-contact-search-before-surfe-v1`  
**Parity with consumer Google AI Mode:** **NOT CLAIMED**

## Root cause (Islas — the smoking gun)

| Stage | What happened |
|-------|----------------|
| Consumer Google | Finds `fislas@hcg.gob.mx` + HCG directorioind |
| Gemini grounding | Also discovers **`fislas@hcg.gob.mx`** (lead) |
| GDI verify (before fix) | Fetched a **Jalisco lookalike** page and accepted **`francisco.islas@jalisco.gob.mx`** |
| Failure class | `SOURCE_EXTRACTION_FAILED` / wrong person association (not “Google failed”) |

**Correction:** When Gemini suggests a specific email, do **not** accept a different lookalike email. Prefer candidate-on-source verification; progressive natural-language follow-up; resolve grounding wrappers when possible.

## Side-by-side (live retest after fix)

| Case | Consumer observed | Gemini discovered | GDI verified | Airtable | Drawer | Failure |
|------|-------------------|-------------------|--------------|----------|--------|---------|
| Islas | fislas@hcg.gob.mx · +52 33 3883 4400 · Ext 51621 | **same email + phone** | **fislas@hcg.gob.mx · +52 33 3883 4400 · Ext 51621** · HCG directorioind | prior / not re-applied this gate | not re-shot this gate | **NONE** (after fix) |
| Maritza | maritza.medina@jalisco.gob.mx · phone+exts | matching email | matching email (+ prior phone path) | prior | not re-shot | **NONE** |
| Alejandra | alejandra.vazquez@igeco.mx · no phone | matching email | matching email · Find Phone only | prior | not re-shot | **NONE** |

Evidence: `LIVE_PARITY.json`, `FOUNDER_PARITY_TABLE.md`.

### Live summary metrics
- Consumer emails known: **3**
- Gemini discovered matching email: **3**
- GDI verified matching email: **3** (was 2 before lookalike fix)
- Incorrect identity matches rejected: lookalike path blocked when Gemini hint present

## Code corrections (canonical resolver — no second engine)

1. Natural-language Google prompts + progressive follow-up (`buildGoogleGroundedFollowUpPrompt`)
2. Grounding wrapper detection + redirect resolve attempt
3. Google candidate email/phone verified only when present on fetched HTML with person match
4. **Reject conflicting emails** when a Gemini candidate email is known
5. Failure-stage taxonomy on discovery metrics (`GDI_PUBLIC_CONTACT_FAILURE_STAGE`)
6. Offline + live parity gate: `npm run test:gdi-google-discovery-parity-v1[:live]`

## Spend (this diagnostic live retest)

~3 discovery runs × (Gemini follow-up + optional Exa). Exact Exa cents in `LIVE_PARITY.json` spendEstimateUsd. Surfe: **0**.

## Still limited

- Many Gemini grounding citations remain `vertexaisearch.cloud.google.com` wrappers; resolve succeeds inconsistently — Exa / official URL ranking still needed.
- Consumer AI Mode can surface LinkedIn/extensions more aggressively than API grounding; LinkedIn is retained when verified, else candidate/uncertain.
- Airtable re-apply + localhost/Railway drawer screenshots still require IC merge/deploy of #484 + authorized persist.
- Full unresolved cohort not re-run in this gate (STOP before expansion).

## Certifications

```
CONSUMER_AI_MODE_PARITY = NOT_CLAIMED
GEMINI_DISCOVERS_FOUNDER_EMAILS = YES (3/3 live)
GDI_VERIFY_AFTER_LOOKALIKE_FIX = YES (3/3)
ISLAS_WRONG_JALISCO_EMAIL = FIXED
OFFLINE_PARITY_GATE = PASS
THREE_CASE_OFFLINE = PASS
SURFE = 0
FULL_COHORT_RERUN = NOT_STARTED
DRAWER_VISIBLE = NOT_ASSESSED_THIS_GATE
```
