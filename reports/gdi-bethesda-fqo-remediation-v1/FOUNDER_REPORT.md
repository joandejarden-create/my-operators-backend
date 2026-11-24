# Bethesda Full E2E / FQO Remediation — Founder Report

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Gold package:** `gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2` (67)  
**Isolated candidate:** `gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261124_5e4d5630` (68 written; 66 gold IDs)  
**Worktree:** `C:\Dev\dealality\wt-gdi-bethesda-fqo-remediation-v1`  
**Branch:** `feat/gdi-bethesda-fqo-remediation-v1`  
**Spend:** $0 · Agent OFF · Airtable OFF · STAGED/CURRENT unchanged · 8080 untouched  

**Freeze preserved:** `C:\Dev\dealality\_gdi_emergency_freeze_20261124-231526`  
**Pointer SHAs (match freeze):**  
- STAGED `f2a4f70b3fb104ee8c59c91648c35e5062de6f29565800a0015f55a2e875e6aa`  
- CURRENT `db4eb0ca7d3e5b08ed160293f085b06260096347e7720f26c4dca6ca4606ce9c`  

---

## 1. Exact fixes for R1–R6

| Root cause | Fix |
|---|---|
| **R1 Evidence inheritance** | `collectEvidenceUrls` + `evaluateEvidenceSupportedDemand` with `HISTORICAL_PRESENTATION_PROVENANCE` / schema-incompatibility path; recover nested provenance; reject unsupported speculation without URLs. |
| **R2 Historical approval compatibility** | Hotel-wide dispositions with internal `RETAINED_HISTORICAL_APPROVAL` / `QUALIFIED_NEW` / `GENUINE_COMMERCIAL_REVIEW` / `TECHNICAL_REMEDIATION_REQUIRED`. Previously approved + verified named person retained unless genuine commercial disqualification. Org-as-person → review. |
| **R3 WHO/HOW** | Named person mandatory for new FQO; labeled organizational route (`LABELED_ORGANIZATIONAL_ROUTE`) allowed when connected to named person; org inbox alone fails; no fabricated names; no paid enrichment. |
| **R4 Validation shortcuts** | Removed narrative-length hotel-fit/action proofs and NYC incumbent hardcode; structured fit, actionable timing, named person, credible pathway, hotel-specific action, market-local competitive context. |
| **R5 PASS→package shrinkage** | Historical PASS may skip strict FQO recheck; truncated/conflicting pills → documented warnings not hard blocks; enrich prose from PASS rows; `hotelWidePassToPackageReconciliation` with `unexplainedDrops: 0`. |
| **R6 Merge pollution** | STAGED/CURRENT/SEED presentation authoritative for timing/thesis/classification; strip research-injected past-only event dates that silently invalidated approved inventory. Historical open accounts (no dated event) keep future pathway under historical mode. |

Leisure categories: **not implemented** (deferred).

---

## 2. FQO rule-by-rule tests

`npm run test:gdi-finalized-qualified-opportunity-v1` → **24/24 PASS**  
`npm run test:gdi-hotel-wide-finalization-stage-v1` → **18/18 PASS**

Covered: labeled org route; org-only fails; short recommendation; historical provenance; speculation reject; Bethesda competitive (no NYC); salesReady non-blocking; serialization/pointer preservation; historical retain vs org-as-person review.

---

## 3. New Bethesda replay count

| Metric | Diagnostic (#596) | After remediation |
|---|---:|---:|
| FQO raw PASS (no historical flag) | 0 | 0 |
| FQO historical PASS | ~14 | **53** |
| Hotel-wide FINALIZATION_PASS (canonical inventory) | 28→20 pkg | **68** (66 gold + 2 new research) |
| Gold IDs in candidate | 20 | **66 / 67** |
| Candidate package written | fail / 20 | **yes / 68** |

---

## 4. Full disposition of all 67 historical IDs

See `reports/gdi-bethesda-fqo-remediation-v1/GOLD_67_DISPOSITIONS.json`.

| Processing disposition | Count |
|---|---:|
| RETAINED_HISTORICAL_APPROVAL | 66 |
| GENUINE_COMMERCIAL_REVIEW | 1 |

| Customer-facing hotel-wide disposition | Count |
|---|---:|
| FINALIZATION_PASS (in candidate) | 66 |
| PREVIOUSLY_APPROVED_REVIEW | 1 |

**Only non-retained gold ID:**

- `gdi_opp_aad_2028_overflow` — AAD Annual Meeting 2028  
  - Primary: **Maritz Global Events** (organization, not a verified named person)  
  - Reasons: `NAMED_COMMERCIAL_PERSON_REQUIRED`  
  - Disposition: `PREVIOUSLY_APPROVED_REVIEW` / `GENUINE_COMMERCIAL_REVIEW`  
  - Evidenced commercial/research gap (WHO), not a technical serialization bug.

Every other original ID is accounted with explicit disposition; **no silent exclusions**.

---

## 5. Technical-regression count before / after

| | Before (#596) | After |
|---|---:|---:|
| TECHNICAL_REGRESSION (estimate) | **46** | **0 unexplained** |
| Gold missing from candidate without disposition | many | **0** |
| Genuine commercial/WHO review remaining | n/a | **1** (AAD/Maritz) |

---

## 6. Contact retention

- `primaryLost` among gold-in-candidate: **0**  
- Type changes: **0**  
- Sales-pill changes: **0**  
- Named contacts + outreach routes preserved from approved presentation on merge.

---

## 7. Opportunity type and sales-pill parity

Field-level compare on 66 retained IDs: **typeChanged 0 · pillChanged 0**.  
Presentation warnings (truncated prose / pill-vs-action conflict) recorded as `WARNING_NOT_EXCLUSION` only — not drops.

---

## 8. PASS-to-package reconciliation

```
passCount: 68
packageCount: 68
unexplainedDrops: 0
warningCount: 30 (truncated / conflicting pills)
```

Research inventory → hotel-wide disposition → qualified candidate → package: auditable at every step (`HOTEL_COMPLETION_MANIFEST.json` + package `hotelWidePassToPackageReconciliation`).

---

## 9. Original-versus-new visual

- Dual-pane HTML: `reports/gdi-bethesda-fqo-remediation-v1/visual/dual-pane-gold-vs-candidate.html`  
- Serve (isolated, not 8080): `node scripts/serve-bethesda-remediation-visual-v1.mjs` → `http://127.0.0.1:8093/`  
- Automated Chromium screenshot in this environment was blocked (sandbox / missing browser binary). Dual-pane HTML is the reviewable visual artifact.

---

## 10. PR / tests / checksums

- Candidate SHA256: `e299e5b6e70d01fc462d26e4697af73036143b029d08ecde42a1638abacefb46`  
- Checksums: `reports/gdi-bethesda-fqo-remediation-v1/CHECKSUMS.json`  
- Tests: FQO 24 PASS · hotel-wide 18 PASS  
- PR: *(filled after open)*  

---

## 11. Remaining genuine commercial research gaps

1. **AAD 2028 overflow** — needs a verified named commercially relevant person (housing provider entity is not FQO-qualified WHO).  
2. Non-gold research HOLD rows (17) from broader inventory — out of gold-67 scope; not forced into candidate.  
3. Soft presentation warnings (truncated prose / conflicting sales pills) on some retained rows — polish only; not eligibility blockers for this remediation.  
4. Six leisure-demand categories remain **integration-audit only** — not executed in this pass.

---

## 12. Founder certification recommendation

**NOT YET READY for founder certification / pointer flip.**

Reasons:
- 66/67 gold IDs reproduced technically; **1 intentional commercial WHO hold** (AAD/Maritz) remains.  
- Candidate is isolated only — STAGED/CURRENT correctly untouched.  
- Certification should wait for founder decision on AAD (recover named contact vs accept review exclusion) and optional prose/pill polish.

**Ready for:** Integration Controller review of this remediation PR; isolated candidate inspection via dual-pane HTML.

**STOP.** No pointer activation. No other hotels. No leisure expansion.

---

## Status

`AWAITING_INTEGRATION_CONTROLLER`
