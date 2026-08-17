# Hotel Equities — PDF Enrichment Plan

**Date:** 2026-07-06  
**Status:** Registration **dry-run ready** — no Airtable writes yet  
**Operator:** Hotel Equities (CALA) — `recWPKu5laVZxsvpn`

> **Authority:** [hotel-equities-source-capture-plan.md](./hotel-equities-source-capture-plan.md), [hotel-equities-extraction-plan.md](./hotel-equities-extraction-plan.md), [partner-intelligence-priority-profile-production-tracker.md](./partner-intelligence-priority-profile-production-tracker.md)

---

## Executive summary

| Question | Answer |
|----------|--------|
| **PDFs on disk?** | **Yes** — 2 files under Operator Reference Material |
| **Already in Source Library?** | **No** — only 3 website HTML rows linked today |
| **Dual-root read works?** | **Yes** — both PDFs resolve from Operator Reference Material |
| **Safe registration path?** | **Narrow script** — `npm run register-hotel-equities-pdf-sources -- --dry-run` |
| **Include in current governance?** | **No** — later enrichment after steward + optional re-publish |
| **Dry-run result (2026-07-06)** | **2 registered**; PDF extract dry-run: 4 proposed / 12 skipped — **manual curation recommended** |

**PDF extraction preview:**

```bash
npm run hotel-equities-extract -- --dry-run --source-group pdf
```

→ `reports/hotel-equities-extract.{md,json}`

**Manual curation (recommended over auto-apply):** [hotel-equities-pdf-manual-curation-plan.md](./hotel-equities-pdf-manual-curation-plan.md)

---

## 1. File inventory

**Operator Reference Material root:**

`G:\My Drive\Dealality™\Platform Design & Build\Operator Reference Material\Hotel Equities CALA\`

| # | On-disk filename | Size | Last modified |
|---|------------------|------|---------------|
| 1 | `HE CALA Marketing Presentation  March 2026.pdf` | 2,832,645 bytes | 2026-05-27 |
| 2 | `Caribbean & Latin America Hospitality Company _ Hotel Equities.pdf` | 139,286 bytes | 2026-05-27 |

**Filename note:** The marketing deck uses **two spaces** before `March` in the on-disk name. `Local File Path` must match exactly.

**Brand Reference Material (existing governed package):**

| Source ID | Title | Local File Path |
|-----------|-------|-------------------|
| `rectG9wdsAeL7u0FG` | Hotel Equities home | `Hotel Equities/website/Hotel Equities home.html` |
| `rec9FSzLhaLPcPvtv` | Hotel Equities Services | `Hotel Equities/website/Hotel Equities Services.html` |
| `recy1oDTNe7kyQGbE` | Hotel Equities CALA | `Hotel Equities/website/Hotel Equities CALA.html` |

---

## 2. Source registration plan

### Why not existing CLI workflows?

| Workflow | Limitation for these PDFs |
|----------|---------------------------|
| `partner-reference:download` | Requires `--url`; downloads into **Brand** Reference Material; files already on disk |
| `partner-reference:harvest-operators` | Captures HTML/PDF to Operator root; **does not** register Source Library rows |
| `syncOperatorReferenceFolder` | Scans **Brand** root only; Hotel Equities not in `PILOT_OPERATORS` registry; folder name mismatch (`Hotel Equities` vs `Hotel Equities CALA`) |

### Recommended method

**Narrow registration script** (allowlisted PDFs only):

```bash
npm run register-hotel-equities-pdf-sources -- --dry-run
```

Apply (founder approval only):

```bash
npm run register-hotel-equities-pdf-sources -- --apply --approve-hotel-equities-pdf-register
npm run hotel-equities-extract -- --dry-run --source-group pdf
```

**Modules:** `lib/partner-intelligence/hotel-equities-pdf-register.js`, `scripts/register-hotel-equities-pdf-sources.mjs`  
**Reports:** `reports/hotel-equities-pdf-register.{md,json}`

Registration creates **Captured** rows only — no auto-approval for extraction or Explorer use.

---

## 3. Proposed Source Library field values

### PDF 1 — HE CALA Marketing Presentation March 2026

| Field | Value |
|-------|-------|
| **Source Title** | HE CALA Marketing Presentation March 2026 |
| **Profile Type** | Operator |
| **Operator / Management Company** | `recWPKu5laVZxsvpn` |
| **Local File Path** | `Hotel Equities CALA/HE CALA Marketing Presentation  March 2026.pdf` |
| **Source Type** | **Operator Capability Deck** |
| **Source Origin** | **Operator Provided** |
| **Region** | **CALA** |
| **Source Quality** | **High** |
| **Status** | **Captured** |
| **Approved for Extraction** | No |
| **Approved for Explorer Use** | No |
| **Verified Source?** | No |
| **Visibility** | Public |

### PDF 2 — Caribbean & Latin America Hospitality Company

| Field | Value |
|-------|-------|
| **Source Title** | Caribbean & Latin America Hospitality Company — Hotel Equities |
| **Profile Type** | Operator |
| **Operator / Management Company** | `recWPKu5laVZxsvpn` |
| **Local File Path** | `Hotel Equities CALA/Caribbean & Latin America Hospitality Company _ Hotel Equities.pdf` |
| **Source Type** | **Operator Capability Deck** |
| **Source Origin** | **Operator Provided** |
| **Region** | **CALA** |
| **Source URL** | `https://www.hotelequities.com/cala.htm` (companion to HTML source `recy1oDTNe7kyQGbE`) |
| **Source Quality** | **Medium** |
| **Status** | **Captured** |
| **Approved for Extraction** | No |
| **Approved for Explorer Use** | No |

---

## 4. Dual-root read verification

`resolveLocalSourceAbsolutePath()` / `readLocalSourceText()` (2026-07-06):

| Local File Path | Resolved root | Readable |
|-----------------|---------------|----------|
| `Hotel Equities CALA/HE CALA Marketing Presentation  March 2026.pdf` | operator | yes (2.8 MB) |
| `Hotel Equities CALA/Caribbean & Latin America Hospitality Company _ Hotel Equities.pdf` | operator | yes (139 KB) |

Brand-root website paths unchanged.

---

## 5. Governance scope

| Package | Include now? | Rationale |
|---------|--------------|-----------|
| **Current applied governance** (3 website sources) | **Keep as-is** | `no_op` after Company Published remap |
| **PDF enrichment** | **Later** | Register → steward → extract → optional governance re-publish |

Do **not** auto-include PDFs in publish scope until:

1. Sources registered and **Approved for Explorer Use = Yes** (human)
2. Facts extracted and **Approved/Edited** (human)
3. Explicit `publish-partner-intelligence-profile-governance` dry-run reviewed

Mixed website + PDF publish scope may remain **Company Published** internally (all company-controlled) but requires founder review before apply.

---

## 6. Target enrichment facts

| Field key | Priority | Supported in registry / narrow extract? | PDF enrichment role |
|-----------|----------|----------------------------------------|---------------------|
| `op.platform.offeredServices` | P0 | **Yes** — primary gap from website extract | **Primary** — marketing deck + services narrative |
| `op.snapshot.companyDescription` | P0 | Yes | CALA positioning (avoid duplicating approved website facts) |
| `op.snapshot.primaryServiceModel` | P0 | Yes | Management / third-party model |
| `op.markets.regionsSupported` | P0 | Yes | CALA footprint, countries/markets |
| `op.brand.familiesOperated` | P1 | Yes | Brand partners on deck |
| `op.capabilities.managementServices` | — | **No** — not in explorer field registry | Use **`op.platform.offeredServices`** proxy |
| `op.ownerValueProposition` | P1 | Listed in narrow extract but **unsupported** in registry | Defer or map to engagement proxies after registry audit |
| `op.operatingModel` | P1 | Unsupported | Defer |

After registration: extend `ALLOWLISTED_SOURCE_IDS` in `hotel-equities-extract.js` (separate change) before PDF extraction apply.

---

## 7. Dry-run registration command

```bash
npm run register-hotel-equities-pdf-sources -- --dry-run
```

**Result (2026-07-06):** `ready=2`, `skip=0`, `blocked=0` — see `reports/hotel-equities-pdf-register.md`.

---

## 8. Recommended sequence

1. **Founder review** this plan + dry-run report.
2. **Apply registration** (when approved): `--apply --approve-hotel-equities-pdf-register`.
3. **Steward** — approve extraction + Explorer use on selected PDF(s) only.
4. **Extend narrow extract allowlist** + dry-run `hotel-equities-extract`.
5. **Apply extract** (separate approval flag).
6. **Re-audit** publish readiness; governance re-publish only if enrichment package approved.

---

## Change Impact

| Tier | **Medium** — read-path already supports files; registration is new Source Library rows when applied |
| Rollback | Delete or reject new Source Library rows; no Setup governance impact until separate publish |

## Regression checklist

- Existing 3 HTML sources still resolve from Brand root.
- Publish readiness for HE remains `no_op` until PDFs approved for Explorer use.
- Do not run broad extraction tools on PDF package without allowlist update.
