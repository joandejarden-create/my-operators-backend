# AVA V3 PDF Version Resolution Certification

**Contract:** `AVA_V3_PDF_VERSION_RESOLUTION_CERTIFICATION_V1`  
**Generated:** 2026-11-05  
**Publication / deployment / share-token rotation / ADP changes:** **NOT authorized**  
**Results artifact:** `AVA_V3_PDF_VERSION_RESOLUTION_RESULTS.json`

---

## 1. Confirmed root cause

The certified Customer Report V3 PDF **does exist** and opens correctly (Waterstone **20 pages**, SHA `e21564771fce…`).

The founder’s download matched the **previous published/archive PDF** because Admin PDF resolution was still bound to the **legacy production store + archive `current.pdf`** when `AVA_STORAGE_MODE` is off (default without durable env):

| Layer | What it pointed at | Waterstone result |
|---|---|---|
| Founder download | Archive `reports/ai-visibility-audit-action-plan/archive/adp_waterstone_boca_raton/current.pdf` | **11 pages** · SHA `8173ca19ace9…` · created 2026-11-05 15:37 CET |
| V3 pilot certification | Durable `data/ava-data/pdfs/adp_waterstone_boca_raton/…customer_v3….pdf` + `current.pdf` | **20 pages** · SHA `e21564771fce…` · 2026-11-05 20:00 CET |
| Active Admin meta (mode=off) | Production store `latestVersionId=ava_v1_phase8_…` → archive `current.pdf` | Served **old** PDF |
| Durable Admin meta (mode=local) | `latestVersionId=…customer_v3…` → durable draft | Served **V3** PDF |

**Exact substitution point:** `GET /api/admin/ai-visibility-audit/:propertyId/pdf` resolved “latest” via `pdfRelativePath` / archive `current.pdf` with **no published-vs-draft channel**. Staging also overwrote a single `current.pdf` alias, so draft and “what Admin downloads” were conflated with the customer-visible artifact depending on which store the process used.

V3 certification PASS was valid for the durable staged artifact; Admin was simply not wired to that channel.

---

## 2. Old versus new PDF hashes

### Waterstone Resort & Marina

| Channel | Pages | SHA-256 | Role |
|---|---:|---|---|
| **Published** (unchanged) | 11 | `8173ca19ace9add954c442bcecb48cade0fb7394de77ea9b5c468b34391e38c8` | Founder-downloaded / customer-visible freeze |
| **New Draft (V3)** | 20 | `e21564771fce6e5631b13934993a38d27bdfe12de36e3cbb66408be5553da944` | Certified Customer Report V3 |

### AC Hotel A Coruña

| Channel | Pages | SHA-256 |
|---|---:|---|
| Published | 13 | `7ed9c2e0925a4dad6489d768921f0a0d5174f1fffc7bddb8c6cb35b6701e2621` |
| New Draft (V3) | 19 | `1c6935e399c3096799611d434dc77c085024d2a5dcfb0548eff260c7e51aa2db` |

### Castillo Hotel Son Vida

| Channel | Pages | SHA-256 |
|---|---:|---|
| Published | 13 | `76944045b5e5…` (full in results JSON) |
| New Draft (V3) | 20 | `f1844ba396ac…` (full in results JSON) |

---

## 3. Storage and route corrections

### Durable channel model

- Module: `lib/ai-visibility-audit-action-plan/ava-pdf-channels-v1.js`
- Channels: `published` · `draft` · `history`
- **No cross-channel fallback.** Missing draft → `DRAFT_PDF_*` 404; missing history → `VERSION_NOT_FOUND` / `HISTORY_PDF_FILE_MISSING` (never returns published).

### Staging

- `stageAvaPdf` writes `{versionId}.pdf` + `draft.pdf` (+ legacy `current.pdf` alias).
- **Never overwrites `published.pdf`.**
- Registers `draftPdfRelativePath` / `draftPdfSha256` / `draftVersionId` on meta.

### Resolution

- `resolveAvaPdfAbsolute` probes `data/ava-data/pdfs/…` even when `AVA_STORAGE_MODE=off` (read-only), so Admin can open durable drafts without silently using archive.
- Durable audit meta is read as an overlay for draft discovery when the active store is still legacy.

### Admin API

- `GET /api/admin/ai-visibility-audit/:propertyId/pdf?channel=published|draft|history`
- Default without `versionId`: **draft**
- With `versionId` and no channel: **history**
- Headers: `X-AVA-PDF-Channel`, `X-AVA-PDF-Sha256`, `X-AVA-PDF-Version-Id`, `X-AVA-PDF-Created`, `X-AVA-PDF-Bytes`
- Hotel detail includes `pdfChannels` for Admin QA (SHA, mtime, availability).

### Admin UI

- Distinct **Published PDF** and **New Draft PDF** Preview/Download controls.
- Table actions default to New Draft.
- Cache-bust: `admin-ai-visibility-audit.js?v=ava-pdf-channels-v1`

### Freeze / sync (local durable)

```bash
npm run ava:v3-pdf-version-resolution-fix
npm run test:ava-pdf-channel-parity
```

Published freeze copies archive `current.pdf` → `pdfs/{propertyId}/published.pdf` without replacing it with V3.

---

## 4. Three-hotel verification

| Hotel | Published SHA | Draft SHA | Expected draft pages | Preview = Download | V3 sections | Pass |
|---|---|---|---:|---|---|---|
| Waterstone | `8173ca19…` (11p) | `e2156477…` | 20 | yes | yes | **PASS** |
| AC A Coruña | `7ed9c2e0…` (13p) | `1c6935e3…` | 19 | yes | yes | **PASS** |
| Castillo | `76944045…` (13p) | `f1844ba3…` | 20 | yes | yes | **PASS** |

Also verified: `AVA_STORAGE_MODE` unset still resolves Waterstone **draft** to V3 SHA and **published** to founder SHA (`modeOffStillResolvesDraft: true`).

Customer share / publication paths were **not** changed; no share tokens rotated. AVA customer distribution remains founder-gated and unpublished for V3.

---

## 5. Local-versus-production status

| Environment | Status |
|---|---|
| **Local durable (`data/ava-data`)** | V3 drafts registered; published frozen; Admin channel resolution verified |
| **Legacy production store / archive** | Published archive bytes **unchanged**; still the customer-visible freeze source for these three |
| **Production / Railway** | **Not deployed.** Until durable volume + this code ship, a production Admin process on old code / empty volume will keep serving archive published PDFs only |

---

## 6. Deployment requirement

To make New Draft Preview/Download work on the hosted Admin:

1. Deploy this code (API + Admin JS + channel module).
2. Ensure durable AVA storage is available (`AVA_STORAGE_MODE=local|required` + `AVA_STORAGE_ROOT` / volume with `pdfs/` + `audits/`).
3. Run `npm run ava:v3-pdf-version-resolution-fix` against that volume (or migrate durable PDF/meta artifacts) so draft pointers and `published.pdf` freezes exist on the volume.
4. **Do not** run a publish/promote step for V3.

Without (1)–(3), production Admin cannot see the local V3 drafts.

---

## 7. Founder authorization requirements

| Action | Authorized now? |
|---|---|
| Preview/Download **New Draft** V3 in Admin (after deploy/sync) | Yes — internal review |
| Replace **Published** with V3 | **No** — requires separate founder publish approval |
| Customer share / email / token rotation | **No** |
| Portfolio V3 apply to remaining hotels | **No** — prior V3 enhancement still awaiting founder review |
| ADP measurement changes | **No** |

---

## Success criterion

> The founder can select New Draft in Admin and Preview/Download the correct certified V3 PDF, while Published and Historical PDFs remain unchanged.

**Locally certified: PASS** for Waterstone, AC A Coruña, and Castillo.  
**Production: pending separate deploy + durable volume sync authorization.**
