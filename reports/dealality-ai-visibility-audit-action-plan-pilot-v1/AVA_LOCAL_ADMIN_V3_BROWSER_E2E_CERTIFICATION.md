# AVA — Local Admin V3 PDF Browser E2E Certification

**Contract:** `AVA_LOCAL_ADMIN_V3_BROWSER_E2E_CERTIFICATION_V1`  
**Generated:** 2026-11-05  
**Base URL verified:** `http://localhost:8080`  
**Publication / deployment / share-token / ADP changes:** **NOT authorized**  
**Results:** `AVA_LOCAL_ADMIN_V3_BROWSER_E2E_RESULTS.json`  
**Screenshots:** `browser-e2e/01-visibility-audit-tab.png`, `02-waterstone-pdf-channels.png`, `03-after-draft-actions.png`

---

## 1. Confirmed root cause

The V3 draft existed and backend channel certification was correct, but the **running localhost:8080 Node process predated the channel API**.

| Fact | Value |
|---|---|
| Stale process | PID **30284** · `node server.js` · started **2026-11-05 19:17 CET** |
| Working directory | `C:\Dev\dealality\my-operators-backend` |
| Predated | PDF channel resolution (`channel=draft\|published\|history`), `X-AVA-PDF-Channel` headers, durable draft overlay |
| Static JS on disk | Already updated (`ava-pdf-channels-v1` / later `v2`) — browser could load new labels |
| Live API behavior (before restart) | `?channel=draft` **ignored**; always returned SHA `8173ca19…` (11 pages); **no** `X-AVA-PDF-Channel` header |

So: UI asked for draft; **old in-memory route handler** still served archive/published bytes. That is why the founder kept downloading the 11-page PDF despite V3 certification PASS.

---

## 2. Running server revision before and after

### Before (broken)

| Field | Value |
|---|---|
| PID | 30284 |
| Started | 2026-11-05T19:17:26 local |
| Draft PDF SHA via API | `8173ca19ace9…` (wrong — published bytes) |
| `X-AVA-PDF-Channel` | absent |
| Artifact | `AVA_LOCAL_ADMIN_SERVER_BEFORE.json` |

### After (fixed)

| Field | Value |
|---|---|
| Command | `AVA_STORAGE_MODE=local node server.js` |
| Started | 2026-11-05T19:24:40Z |
| Repo | `C:\Dev\dealality\my-operators-backend` |
| `DEALALITY_GIT_HEAD` | `cc62f243758a` |
| Draft Waterstone SHA | `e21564771fce…` · **20 pages** · header `channel=draft` |
| Published Waterstone SHA | `8173ca19ace9…` · **11 pages** · header `channel=published` |
| Artifact | `AVA_LOCAL_ADMIN_SERVER_AFTER.json` |

**No production deploy.** Local process only restarted.

---

## 3. API channel results (live localhost:8080 after restart)

| Hotel | Channel | Pages | SHA-256 | Header |
|---|---|---:|---|---|
| Waterstone | published | 11 | `8173ca19ace9add954c442bcecb48cade0fb7394de77ea9b5c468b34391e38c8` | `published` |
| Waterstone | draft | 20 | `e21564771fce6e5631b13934993a38d27bdfe12de36e3cbb66408be5553da944` | `draft` |
| AC A Coruña | published | 13 | `7ed9c2e0925a…` | `published` |
| AC A Coruña | draft | 19 | `1c6935e399c3…` | `draft` |
| Castillo | published | 13 | `76944045b5e5…` | `published` |
| Castillo | draft | 20 | `f1844ba396ac…` | `draft` |
| Waterstone history missing | history | — | — | **404** `VERSION_NOT_FOUND` (no fallback) |

Hotel metadata includes `pdfChannels.draft` / `pdfChannels.published` with matching SHAs.

---

## 4. Browser network request results

Playwright E2E against the served Admin app recorded:

- **7** requests with `channel=draft`
- **3** requests with `channel=published`
- UI click **Download New Draft PDF** saved bytes SHA `e21564771fce…` (exact certified V3)
- Missing history from browser context: **404** / `VERSION_NOT_FOUND`

Admin HTML cache bust verified: `admin-ai-visibility-audit.js?v=ava-pdf-channels-v2`.

---

## 5. Downloaded PDF hashes (browser context)

| Hotel | Draft SHA | Published SHA | Preview = Download |
|---|---|---|---|
| Waterstone | `e21564771fce…` | `8173ca19ace9…` | yes |
| AC A Coruña | `1c6935e399c3…` | `7ed9c2e0925a…` | yes |
| Castillo | `f1844ba396ac…` | `76944045b5e5…` | yes |

Files under `browser-e2e/*-draft.pdf` / `*-published.pdf` and `waterstone-ui-click-draft.pdf`.

---

## 6. Page counts

| Hotel | Draft pages | Published pages |
|---|---:|---:|
| Waterstone | **20** | **11** |
| AC A Coruña | **19** | **13** |
| Castillo | **20** | **13** |

Waterstone draft text includes **Digital Platform** / platform-by-platform assessment and **Implementation by Responsible Party**.

---

## 7. Screenshots of corrected Admin controls

| File | Shows |
|---|---|
| `browser-e2e/01-visibility-audit-tab.png` | Visibility Audit tab loaded (38 hotels); PDF column **Draft + Published** |
| `browser-e2e/02-waterstone-pdf-channels.png` | Waterstone drawer **PDF Report**: Published PDF + New Draft PDF with separate Preview/Download; draft version `ava_v1_customer_v3_…`; published SHA prefix `8173ca19…` |
| `browser-e2e/03-after-draft-actions.png` | After draft download actions |

**Founder path (localhost):**

1. Hard-refresh Admin (`Ctrl+Shift+R`) so `ava-pdf-channels-v2` loads.
2. AI Visibility Audit → open **Waterstone** → **PDF Report**.
3. Click **Preview New Draft PDF** / **Download New Draft PDF** (not the ambiguous table icon alone if unsure).
4. Expect **20 pages** / SHA `e2156477…`.
5. **Published** controls remain **11 pages** / `8173ca19…`.

---

## 8. Browser E2E test results

```bash
npm run test:ava-local-admin-v3-browser-e2e
```

**PASS** (`AVA_LOCAL_ADMIN_V3_BROWSER_E2E_RESULTS.json`).

Permanent regression covers:

- Published + New Draft controls visible in served Admin UI  
- Controls/API use correct `channel=`  
- Preview/Download identical draft bytes  
- Draft SHAs match certified V3  
- Published SHAs unchanged  
- No silent channel fallback  
- Runs against actual `localhost:8080` Admin application  

Backend channel parity remains: `npm run test:ava-pdf-channel-parity`.

---

## 9. Remaining deployment requirements

| Item | Status |
|---|---|
| Local Admin V3 draft Preview/Download | **Verified PASS** after server restart |
| Production / Railway | **Not deployed** — requires separate authorization + durable volume sync |
| Publish V3 over published PDF | **Not done / not authorized** |
| Share token rotation | **Not done** |
| ADP measurements | **Unchanged** |

If localhost is started again without this code loaded into memory, restart with:

```powershell
$env:AVA_STORAGE_MODE='local'; node server.js
```

---

## Success criterion

> The founder can use the actual localhost:8080 Admin interface to preview and download the 20-page Waterstone V3 draft, while the 11-page published version remains unchanged.

**PASS** — verified via live API on the restarted server **and** Playwright browser E2E against the served Admin UI (including UI click download SHA match).
