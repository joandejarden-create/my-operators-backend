# HE Contact Intelligence — prevent cross-worktree runtime writes (proposal)

**Status:** FOUNDER REVIEW — do not Integration-merge until approved  
**Stream:** Hotel Explorer (`ws-hotel-explorer`)  
**Date:** 2026-11-19  
**Related:** HE Tabs 1–5 inventory wrote CI owner routes into `ws-adp` because audits hit the ADP/Integration server on `:8080`

---

## Diagnosis

| Question | Finding |
|---|---|
| Which local server do HE research/audit scripts use? | Previously defaulted to `http://127.0.0.1:8080` (Integration/ADP canonical). HE product servers use isolated ports (e.g. `8091`) with `DEALALITY_ALLOW_WORKTREE_SERVER=1`. |
| Working directory / effective CI data root | Contact Intelligence store resolves to `<serving-process-cwd>/data/hotel-intelligence/contact-intelligence` unless `CONTACT_INTELLIGENCE_DATA_ROOT` / `HOTEL_INTELLIGENCE_DATA_DIR` is set. A GET against ADP’s process therefore wrote under `ws-adp`. |
| Root cause | (1) HE audit default BASE = `:8080` foreign server; (2) `hotelGet` write-on-read called `putOwnerRoute` during GET. |

Airtable-first persistence for hotel graph facts is unchanged. Local CI files remain **staging only** (not Census / HI graph authority). **GSF contacts are not migrated to Airtable** in this change.

---

## Proposed configuration (reuse existing infra)

Precedence (unchanged contract, now documented + affinity-guarded):

1. `CONTACT_INTELLIGENCE_DATA_ROOT`
2. `HOTEL_INTELLIGENCE_DATA_DIR/contact-intelligence`
3. `<serving-repo>/data/hotel-intelligence/contact-intelligence`

**Deliberate HE local runtime root:**

```bash
CONTACT_INTELLIGENCE_DATA_ROOT=C:/Dev/dealality/ws-hotel-explorer/data/hotel-intelligence/contact-intelligence
CONTACT_INTELLIGENCE_ALLOW_CROSS_WORKTREE=0
HE_LOCAL_BASE_URL=http://127.0.0.1:8091
PORT=8091
DEALALITY_ALLOW_WORKTREE_SERVER=1
```

Start HE server from `C:/Dev/dealality/ws-hotel-explorer` only. Do not point HE audits at Integration/ADP `:8080`.

Escape hatch (founder-only): `CONTACT_INTELLIGENCE_ALLOW_CROSS_WORKTREE=1`.

---

## Code changes (this branch)

1. `lib/hotel-intelligence/contact-intelligence/data-root.js` — resolve root + refuse foreign registered worktree affinity.
2. `store.js` — uses affinity assert; lazy mkdir on write.
3. `service.js` — **removed** `putOwnerRoute` from GET/`applyCurrentOwnerReuse` (no write-on-read).
4. `server.js` `/api/dev/runtime-identity` — exposes CI data-root source, stream affinity (basename trail only; no full paths).
5. `scripts/lib/he-local-runtime-target.mjs` + audit script — default BASE `8091`; fail-closed unless identity is HE.
6. `.env.example` — documents the variables above.
7. Gate: `npm run test:he-contact-intelligence-data-root-affinity-v1`

**Out of scope / preserved:** ADP authentication and ADP report functionality; GSF → Airtable contact migration.

---

## Validation results

See companion JSON: `HE-CONTACT-INTELLIGENCE-RUNTIME-DATA-ROOT-VALIDATION-V1.json` (filled by validation run).

Expected gates (validation JSON `passed: true` @ 2026-11-19):

- [x] Affinity: HE→HE root OK; HE→`ws-adp` root refused
- [x] `createContactStore({ root: adpPath })` throws `CONTACT_INTELLIGENCE_CROSS_WORKTREE_DATA_ROOT`
- [x] `hotelGet` creates **zero** owner/hotel files under a temp store
- [x] Default HE audit BASE = `http://127.0.0.1:8091`
- [x] Live: HE `:8091` runtime-identity `servingDirectoryBasename=ws-hotel-explorer`, `servingStream=he`, `affinityOk=true`, `dataRootSource=MODULE_REPO_DEFAULT`
- [x] Live GET contacts did not grow CI `owners/` file count
- [x] Audit mock/refusal: ADP basename → `foreign_runtime_target`; unreachable `:8080` → `runtime_identity_unreachable`

---

## Founder decision

Approve to land with HE PR (or as follow-on HE commit) after reviewing validation JSON. Integration Controller merge only after approval — **this document is not merge authorization**.
