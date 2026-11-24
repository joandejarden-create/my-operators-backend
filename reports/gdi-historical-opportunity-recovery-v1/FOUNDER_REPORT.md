# P0 GDI Historical Opportunity Recovery — Founder Report

**As of:** 2026-11-24  
**Freeze held:** no STAGED/CURRENT activation, no package deletion, no paid research, no Airtable writes  
**Serve:** `C:\Dev\dealality\wt-gdi-hotel-wide-finalization-v1` @ localhost:8080  
**Artifacts:** `reports/gdi-historical-opportunity-recovery-v1/`

---

## 1. Exact root cause and first breaking commit

| Layer | Cause |
|-------|--------|
| **Primary (founder local wipe)** | Commit **`284c7c795`** — `fix(gdi): founder localhost serves FQO-only inventory` applied **runtime six-gate FQO filtering** to **already-approved STAGED/CURRENT packages**, then (when FQO yielded 0) **replaced** them with smaller certified candidates (e.g. Bethesda 67→19). |
| **Secondary (product intent)** | PRs **#593 / #594** removed Research View as the main founder surface; hotels **without** STAGED/CURRENT (Hilton research 45, Wyndham SD 82, Waterstone 29) no longer show research inventory by default. That is **data hiding by product policy**, not package deletion. |
| **Not data loss** | Historical packages, research stores, STAGED/CURRENT pointers, and CURRENT share payloads remain on disk. |

**Classification:** mostly **DATA HIDING / SCHEMA MISMATCH**, not deletion.

Dominant false FQO rejects on legacy packages:

- `MISSING_EVIDENCE_URLS` (Bethesda: evidence often not in `sourceUrls` shape the new gate expects)
- `PERSON_SPECIFIC_CHANNEL_REQUIRED` (Radisson/YOTEL/Alltra: named contacts present; channels often only in research/overlays, not package DTO)

---

## 2. Last-known-good version per focus hotel

| Hotel | LKG source | Version / path | LKG count |
|-------|------------|----------------|-----------|
| Bethesda | STAGED=CURRENT | `gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2` | **67** |
| Radisson Toronto | STAGED=CURRENT | `gdi_pub_recH9h3McgMNFQEla_20261116_drawer_truncation_remediation_v1` | **66** |
| YOTEL Geneva | STAGED=CURRENT | `gdi_pub_recrPQcZg7SFARRb2_20261117_yotel_geneva_lake_exa_30usd_scale_v1` | **42** |
| Wyndham Alltra PC | STAGED | `gdi_pub_recXh4NgF8sDbi9cQ_20261114_wyndham_alltra_20usd_full_e2e_v1` | **27** |
| Hilton NYTS | RESEARCH_STORE (no STAGED) | `data/.../rec35fExUxCClpOP6/opportunities.json` | **45** (+ paid candidate **6**) |
| Wyndham San Diego | RESEARCH_STORE | `recnPB3Q02NO8Ma1o` | **82** |
| Waterstone | RESEARCH_STORE | `recgMYovrrZDJMqzX` | **29** |

---

## 3. Historical-versus-current count matrix

| Hotel | LKG | Strict FQO (broken) | Legacy-compat restored | Customer CURRENT share |
|-------|-----|---------------------|------------------------|------------------------|
| Bethesda | 67 | 0 (then candidate 19) | **67** | **67** intact |
| Radisson Toronto | 66 | 4 | **66** | **66** intact |
| YOTEL | 42 | 2 | **42** | **42** intact |
| Wyndham Alltra | 27 | 0 | **27** | no CURRENT |
| Hilton | 45 research / 6 paid | 6 candidate | research not auto-shown; paid-6 available as candidate | no CURRENT |
| Wyndham SD | 82 research | n/a | research not auto-shown (no STAGED) | no CURRENT |
| Waterstone | 29 research | n/a | research not auto-shown (no STAGED) | no CURRENT |

Full JSON: `PORTFOLIO_RECONCILIATION_MATRIX.json` + per-hotel `*.suppressions.json`.

---

## 4. Opportunity-ID-level suppression analysis

Per-hotel suppression files list every LKG row that **new FQO would hide**, with:

- original opportunity ID  
- previously displayed named contact  
- previously displayed channel (email/phone/LinkedIn when present on package)  
- classification (`CONTACT_FIELD_MISMATCH`, `MISSING_SOURCE_EVIDENCE`, `EXPIRED_OR_HISTORICAL`, `FOUNDER_REJECTED`, `FILTERED_BY_NEW_FQO`)  
- rejection reasons from the new evaluator  

Example (Bethesda): **67/67** would fail new FQO mainly on `MISSING_EVIDENCE_URLS*` despite **67 named** and **50 with some channel** on the approved package.

---

## 5. Recovered named contacts and channel coverage (LKG packages)

| Hotel | Named on LKG | With any channel field on package |
|-------|--------------|-----------------------------------|
| Bethesda | 67/67 | 50/67 |
| Radisson | 66/66 | 4/66 |
| YOTEL | 42/42 | 2/42 |
| Wyndham Alltra | 27/27 | 0/27 |

Contacts were **not deleted**. Package DTOs often lack person-specific channel fields the new gate requires; research/WHO/HOW overlays remain separately.

---

## 6. Actual data-loss vs filtering

| Kind | Finding |
|------|---------|
| Package deletion | **None** observed |
| Pointer mutation | **None** (freeze held) |
| Founder-local filtering | **Yes** — `284c7c795` runtime FQO + candidate fallback |
| Customer CURRENT content | **Unchanged** for Bethesda/Radisson/YOTEL (share resolver never applied runtime FQO) |
| Research-only hotels | Visibility drop from **Research View removal** (#593/#594), not package loss |

---

## 7. Customer-link regression results

Audit: `CUSTOMER_SHARE_LINK_AUDIT.json`

- Bethesda / Radisson / YOTEL **CURRENT** packages still resolve with full LKG counts (67/66/42).  
- Runtime FQO wipe was **founder `getGdiOpportunities` only**.  
- Hotels without CURRENT (Hilton, Wyndham SD, Waterstone, Alltra) have no CURRENT share package — pre-existing, not caused by `284c7c795`.  
- **No CURRENT pointer changes** made as workaround.

---

## 8. Recommended safe compatibility fix (implemented)

**Retain strict FQO for new candidacy.**  
**For identifiable approved STAGED/CURRENT previews:** apply **soft safety only** (exhibitor placeholders + explicit founder reject/disqualify) — module `gdi-legacy-approved-inventory-compat-v1.js`.

Do **not** replace legacy STAGED with a smaller hotel-wide candidate when FQO fails.

New opportunities / non-activated candidates remain FQO-gated.

---

## 9. Restored localhost:8080 screenshots

| Hotel | Result | Screenshot |
|-------|--------|------------|
| Bethesda | **67 of 67** | `page-2026-11-24T21-19-29-200Z.png` |
| Radisson Toronto | **66 of 66** | `page-2026-11-24T21-19-49-390Z.png` |
| YOTEL Geneva | **42 of 42** | (captured same session; Showing 42 of 42) |

Hard-refresh with `app.js?v=gdi-legacy-approved-inventory-compat-v1`.

---

## 10. Remaining genuinely unqualified / open gaps

1. **Hilton / Wyndham SD / Waterstone:** no approved STAGED/CURRENT — founder local view will not resurrect full research inventories without either a governed STAGED package or an explicit Research admin escape (`?gdiAdminResearch=1&live=1`).  
2. **Package channel gaps:** many approved rows have names but incomplete person-specific channels in the package DTO — remediate via contact overlay merge (no inventing), then optional FQO re-cert for *new* candidacy.  
3. **Hilton paid-6** remains the certified FQO candidate; not a substitute for the 45 research rows.  
4. Soft-safety still excludes exhibitor placeholders / explicit rejects only.

---

## 11. PR and CI evidence

| Item | Status |
|------|--------|
| PR | [#595](https://github.com/joandejarden-create/my-operators-backend/pull/595) |
| Prior tip | `284c7c795` (introduced wipe) |
| Recovery commit | (this push) legacy-approved compat + forensic matrix + tests |
| Tests | `test:gdi-legacy-approved-inventory-compat-v1`, `gdi:historical-opportunity-recovery-reconcile-v1` |
| Freeze | No STAGED/CURRENT activation performed |

---

**STOP for founder review before any STAGED/CURRENT pointer changes or customer republication.**
