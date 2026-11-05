# AVA Autonomous Evidence Resolution — Certification

**Generated:** 2026-11-05  
**Contract:** `AVA_AUTONOMOUS_EVIDENCE_RESOLUTION_V1` · `AVA_FULL_E2E_PIPELINE_V1`  
**Paid research:** $0 · **Publication:** not authorized · **Deployment:** none  
**Founder approvals changed:** no  

## Verdict

**PASS_WITH_LIMITATIONS**

Permanent autonomous evidence verification and remediation is integrated into the AVA Full E2E pipeline as a **mandatory** stage. All six existing audits pass the evidence gate under the new engine. Limitations: same-domain replacement search is heuristic (no paid discovery APIs); some CDNs still require real-browser verification under Cloudflare challenges.

---

## 1. Architecture changes

| Component | Role |
|---|---|
| `claim-evidence-graph-v1.js` | Claim ID ↔ evidence URL ↔ recommendation traceability |
| `autonomous-evidence-resolution-v1.js` | Attempts 1–5 remediation loop with budgets |
| `ava-full-e2e-pipeline-v1.js` | Canonical 14-step contract; stages 6–11 mandatory / non-skippable |
| `evidence-verification-escalation-v1.js` | L2 Playwright + timestamped cache (reused) |
| `evidence-hyperlink-certification-v1.js` | Final gate + PDF annotation compare (wired to probeFn / escalation) |
| `lifecycle-v1.js` | `AVA_EVIDENCE_PIPELINE_STATUSES` substates on meta |
| `api/admin-ai-visibility-audit.js` | Admin validate + PDF generate run mandatory stages |
| `public/js/admin-ai-visibility-audit.js` | Certification summary + Evidence Resolution Log |

### Canonical Full E2E sequence (enforced)

1. Load certified ADP baseline  
2. Research hotel / competitive positioning  
3. Generate evidence-backed findings  
4. Identify recommended actions  
5. Digital Authority analysis (when available)  
6. **Verify all evidence URLs** *(mandatory)*  
7. **Autonomous remediation** *(mandatory)*  
8. **Revalidate** *(mandatory)*  
9. **Generate staged PDF** *(mandatory)*  
10. **Validate PDF hyperlinks** *(mandatory)*  
11. **Repair PDF links / regenerate** *(mandatory)*  
12. Final audit-quality certification  
13. Stage for founder review  
14. Explicit founder approval before customer publication  

`skipEvidenceStages: true` throws `AVA_EVIDENCE_STAGES_CANNOT_BE_SKIPPED`.

### Independent safeguards preserved

| State | Meaning |
|---|---|
| `EVIDENCE_VALIDATION_PENDING` / `EVIDENCE_REMEDIATION_RUNNING` / `EVIDENCE_VALIDATED` | Pipeline substates (`meta.evidencePipelineStatus`) |
| `VALIDATION_REQUIRED` | Material evidence unresolved |
| `AUDIT_VALIDATED` | Technical certification only |
| `FOUNDER_APPROVED_FOR_CUSTOMER` | Explicit founder gate — **never** set by automation |

Automation never alters certified ADP measurements, never publishes, never grants founder approval.

---

## 2. Automated resolution strategies

| Attempt | Strategy |
|---|---|
| 1 | Retry / redirect / URL normalize with bounded backoff |
| 2 | Playwright browser verification (no CAPTCHA/auth bypass) |
| 3 | Same-domain replacement candidates + claim-keyword check (homepage alone rejected unless content supports claim) |
| 4 | Qualify / rewrite / remove unsupported statements; stage founder-reviewed diffs |
| 5 | `REQUIRES_MANUAL_REVIEW` with attempt log + suggested next actions |

Budgets (defaults): HTTP retries 2 · browser attempts 8 · replacement searches 12 · claim rewrites 20 · PDF regenerations 2 · elapsed 180s · paid research $0.

---

## 3. Before / after — six existing audits

| Hotel | Mode | Before | After | Gate | Auto-repaired this run |
|---|---|---|---|---|---|
| Waterstone Resort & Marina | remediate | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 |
| Castillo Hotel Son Vida | remediate | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 |
| Sheraton Mallorca Arabella Golf | remediate | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 |
| Wyndham San Diego Bayside | remediate | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 |
| Spice Island Beach Resort | remediate | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 |
| Bethesda Marriott | regression | `AUDIT_VALIDATED` | `AUDIT_VALIDATED` | PASS | 0 |

Prior recovery work already cleared the 11 outstanding blockers; this certification confirms the **permanent engine** re-validates all six without regressions and without creating duplicate report versions when nothing changed (`versionsCreated: 0`).

Results JSON: `AVA_AUTONOMOUS_EVIDENCE_SIX_HOTEL_RESULTS.json`

---

## 4. Issues resolved without manual intervention

| Metric | Value |
|---|---:|
| Six-hotel gate failures after engine | **0** |
| Founder interventions required this run | **0** |
| New PDF versions (no-op when clean) | **0** |
| Prior recovery auto-resolved checks (context) | **11 / 11** |

---

## 5. Claims revised or removed (this certification run)

None — all six payloads already held gate-passing evidence from the prior recovery pass. The engine recorded `EVIDENCE_VALIDATED` pipeline status without further claim rewrites.

---

## 6. Remaining blockers

None for the six certified hotels.

Operational limitations (not gate failures):

- Headless Playwright may still hit Cloudflare on some hosts; real-browser / cached `REAL_BROWSER_VERIFIED` results remain valid when claim support was confirmed.
- Replacement discovery is same-domain heuristic only until a founder-authorized paid provider is approved.
- No invented backlink/SEO metrics.

---

## 7. Research costs

| Item | Amount |
|---|---:|
| Paid APIs / subscriptions | **$0** |
| Estimated per-hotel autonomous pass | ~30–90s wall clock (cache warm) |
| Scale to 38 hotels | Same engine; no new paid tools required |

---

## 8. Test results

`node scripts/test-ava-autonomous-evidence-resolution-v1.mjs`

| # | Scenario | Result |
|---|---|---|
| 1 | HTTP 404 + replacement/repair | PASS |
| 2 | HTTP 403 + browser verify | PASS |
| 3 | HTTP 403 + no verification | PASS |
| 4 | HTTP 200 without claim support | PASS |
| 5 | Redirect unrelated | PASS |
| 6 | Outdated source | PASS |
| 7 | Unsupported claim revision | PASS |
| 8 | Multiple claims / one URL | PASS |
| 9 | Rejected URL in PDF | PASS |
| 10 | PDF annotation mismatch | PASS |
| 11 | Bot-blocked no replacement | PASS |
| 12 | Founder-reviewed remediation diff | PASS |
| 13 | Provider outage retry | PASS |
| 14 | Budget/retry limit | PASS |
| 15 | Repeat validation no duplicate PDF version | PASS |
| — | Skip guard | PASS |
| — | Full E2E stage mandatory flag | PASS |

**17 / 17 passed.**

---

## 9. Readiness for 38 hotels

Ready to apply as the mandatory evidence stage for every newly generated audit and Admin PDF regenerate path.

Rollout priority (unchanged; **do not** mass-regenerate 38 PDFs solely to test):

**Wave 1** — AC Coruña · Chapultepec · Condesa Cancún · Waterstone · Castillo · Sheraton Mallorca  
**Wave 2** — Ambridge ×3 (canonical IDs) · Bethesda  
**Wave 3** — Remaining eligible portfolio  

AI Admin now shows: total checked · auto-verified · auto-repaired · claims revised · unresolved · last verification · certification status · expandable Evidence Resolution Log.

---

## Acceptance checklist

| Criterion | Result |
|---|---|
| Zero known broken customer-facing links (six hotels) | PASS |
| Zero unsupported customer-facing claims | PASS |
| Zero unresolved material blockers | PASS |
| Correct PDF hyperlink annotations | PASS |
| Claim-to-evidence traceability | PASS |
| Founder/publication safeguards preserved | PASS |
| Stages 6–11 non-skippable | PASS |
| No unauthorized paid research | PASS |

**Final verdict: PASS_WITH_LIMITATIONS**
