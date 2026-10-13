# Bethesda Pilot — Client Delivery Closure

**Date:** 2026-10-02  
**Working tree base SHA:** `ab00bf27784d997fff709c3d24b7d0ffa27fd320`  
**Production deployment ID:** `6de62ff2-a4dd-4439-b1de-f7477b80f3f3`  
**Production deployed this run:** **YES** (hotfix overlay WT on `ab00bf27`)  
**Production revision match:** **YES** (expected hotfix on `ab00bf27` → live deployment `6de62ff2`)

---

## A. Executive Result

| Gate | Result |
|---|---|
| ADP PDF | **PASS** (Oct 1 baseline KPIs; production render HTML verified; 6 pages; no internal IDs) |
| GDI PDF | **PASS** (production binary 243,766 bytes `%PDF-`; View/Download via share) |
| GDI storage | **PASS** — generate-on-miss + versioned durable store; binary 404 resolved |
| Canonical cover | **PASS** — BAS/DRS cover shell; GROUP & DEMAND INTELLIGENCE; Washington metropolitan area; human date |
| Report Archive | **PASS** — Admin tab + API + Bethesda golden entries with checksums |
| ADP external | **PASS** (unauthenticated share resolve + report) |
| GDI external | **PASS** (resolve + opportunities + demand report + PDF) |
| Rad ready | **YES — SAFE TO SEND** |

---

## B. GDI PDF Storage Root Cause

| Field | Value |
|---|---|
| Cause | **EPHEMERAL_PATH / ARTIFACT_NOT_ON_PRODUCTION_FS** + no generate-on-miss |
| Classification | `OBJECT_NOT_FOUND` secondary to lean-deploy omit + ephemeral disk |
| Fix | Env-overridable durable root; versioned archive copies; share `report-pdf` **generate-once-and-persist on miss**; Report Archive ledger; `.railwayignore` allowlist for PDF/archive artifacts |
| Persistent storage | Prefer `DEALALITY_REPORT_PDF_ROOT` Railway volume; shipped Bethesda binaries in this deploy as belt-and-suspenders |

---

## C. ADP PDF

| Field | Value |
|---|---|
| Production verified | **YES** (render HTML on production; PDF generated against prod render) |
| Baseline displayed | Consideration **42.1%**, Scenario Presence **81%**, Oct 1 labeling present |
| Visual QA | Chrome (filters/Load Report) hidden; no admin/internal IDs |
| Archive | Oct 1 structured baseline `adp_baseline_2026-10-01_v1` (no fabricated historical PDF); current client PDF `adp_current_2026-10-02_v1` |

---

## D. GDI PDF

| Field | Value |
|---|---|
| Cover parity | **PASS** |
| Body QA | **PASS** (16 pages) |
| Content fixes | RNA watch dedupe **PASS**; nav-title scrub **PASS**; ACTS WHO≠HOW provenance **PASS**; ACC → Association **PASS**; no raw DMV **PASS** |
| Storage/retrieval | Production share PDF **PASS** |

Archive: Day-1 structured `gdi_day1_2026-10-01_v1` (ready=37); current client PDF `gdi_current_2026-10-02_v3` (supersedes v2)

---

## E. Report Archive

| Field | Result |
|---|---|
| Tab | **PASS** |
| ADP history | Oct 1 baseline snapshot + current PDF v1 |
| GDI history | Day-1 snapshot (37) + current PDF v2/v3 |
| Structured snapshots | **PASS** |
| Checksums | **PASS** on retrieve |
| Retrieval | Oct 1 ADP consideration=42.1; Oct 1 GDI ready=37 (live 35) |

---

## F. Production Client Smoke

| Surface | Result |
|---|---|
| ADP external | **PASS** |
| GDI external | **PASS** |
| GDI Opportunities | **PASS** (35) |
| GDI Demand Report | **PASS** (`/pdf-report`) |
| ADP PDF | **PASS** |
| GDI PDF | **PASS** |
| Unauthenticated | **PASS** (share capability only; no Admin auth) |

---

## G. Rad Package

**READY TO SEND** — see `RAD_FINAL_SEND_PACKAGE.md` and `RAD_FOLLOW_UP_EMAIL.md`.

---

## H. Integrity

| Check | Result |
|---|---|
| Oct 1 ADP baseline unchanged | **YES** |
| GDI Day-1 snapshot unchanged | **YES** (ready=37) |
| Attributes unchanged | **YES** |
| Opportunity IDs unchanged | **YES** |
| Token unchanged | **YES** (`gdisht_47c25d74c79216021fb36150`) |

---

## FINAL VERDICT

**BETHESDA CLIENT DELIVERY PASSES — READY TO SEND RAD**
