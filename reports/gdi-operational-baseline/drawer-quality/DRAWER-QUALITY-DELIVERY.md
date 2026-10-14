# Drawer quality delivery — Mayflower repair

**As of:** 2026-10-15  
**Commits expected on branch:** `gdi/parallel-provider-san-diego`  
**Airtable:** Mayflower (`rec0eTEnCqlsGJJt9`) updated via canonical importer  
**Local browser:** verified on `http://localhost:3080`  
**Production browser:** **blocked** (requires `railway login` + deploy of UI assets)

---

## 1. Content repaired and persisted (Airtable)

All **11 facing** Mayflower cards refreshed with `forceEditorial` overlay:

`reports/gdi-operational-baseline/drawer-quality/mayflower-drawer-quality-overlay.json`

Apply artifacts: `drawer-quality/mayflower-apply/` (backups + import-result-apply-*).

| Card | Substantive change |
|------|--------------------|
| **ACTS TS27** | FACT vs HYPOTHESIS on DC city / unnamed venue; Mayflower Conn Ave + K Street HQ rationale; Bethesda diversion limitation; Elizabeth Lancaster action; venue history from ACTS past meetings |
| AHIMA, NDSS, ACC, AMWA, HCAOA, CMSS, NADO 2028 | Distinct travelers / fit / why-now / actions; hotel-specific tokens |
| NCHV, NADO 2027, NAR GAD | Overflow / stretch posture explicit — no primary-host displacement |

**Contract after apply:** drawerCompletenessPass **11 / 11** (v3 substantive checks).

**Reimport:** `forceEditorial` + richer overlay wins over thin stored copy; feedback / IDs / unmatched rows preserved.

---

## 2. Rendering fixes (shared UI)

- Deduped drawer sections; FACT/hypothesis and limitations sections
- Humanized `FUTURE_CYCLE` → Association occasion / Future cycle expected
- Removed customer-facing “canonical scoring” implementation notes
- Qualitative fit when no numeric score (no empty score columns)
- Fit / evidence confidence / sourcing / sales action kept distinct
- Validation / actions / outcomes panel preserved

Screenshots: `drawer-quality/screenshots/mayflower-acts-drawer-final.png`

---

## 3. Missing research still unresolved

| Gap | Status |
|-----|--------|
| ACTS TS27 host hotel / exact venue | Unknown — labeled hypothesis only |
| Peak rooms / attendance for most Mayflower cards | Unknown (acceptable) |
| Prior host hotels (beyond package notes) | Partially noted; not exhaustively researched |
| Bethesda Premier Cup CONTACT_NOW + evidenceConfidence 0.85 vs weaker contact confidence | **Audited, not rewritten** — do not copy as open-demand model |
| Waterstone / Renaissance deep editorial rewrite | Not in this Mayflower pass — shared renderer applied |

---

## 4. Rendering deployed vs local only

| Surface | Status |
|---------|--------|
| Airtable payload / detail API | **Persisted** |
| Local browser (`localhost:3080`) | **Verified** (ACTS + list; Bethesda list loads) |
| Production Railway UI | **Not deployed** — login required |

---

## 5. Production browser verification

**Blocked.** Same user action as prior delivery:

```powershell
railway login
railway whoami
npm run deploy:production
```

Then verify production Mayflower ACTS drawer + one Bethesda drawer for regressions.

---

## 6. Build-time quality (v3)

`drawer-content-contract-v1.js` now separately reports:

- qualification  
- contact completeness  
- drawer content  
- **drawer quality** (repeated identical prose, missing hotel-specific rationale, unsupported verified claims, contact relevance gaps, implementation notes)

Text presence alone no longer passes.

---

## Acceptance

Mayflower ACTS is a coherent sales brief that survives Airtable → detail API → drawer render with labeled facts/hypotheses, hotel-specific fit, limitations, contact route, and a concrete next ask. Schema pass alone is not the claim — local browser proof is.
