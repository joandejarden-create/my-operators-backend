# Internal Resources Hub (Admin)

**Date:** 2026-07-03  
**Audience:** Dealality platform administrators  
**FPP:** Task 2.02 (scoring weights), GTM Pilot Wave 1 resources

---

## Where internal documentation lives

| Layer | Purpose |
|-------|---------|
| **Repo (`docs/`)** | Authoring source — markdown, field matrices, audits. Version-controlled. |
| **Google Drive (`Dealality™`)** | Copy/paste Word artifacts for outreach (EN + ES). Generated via `scripts/generate-gtm-resources-docx.py`. |
| **Platform (admin-only)** | Live reference under **Support** — same auth as Owner Pilot Runbook. |

### Platform routes (admin only)

**Left nav:** **Settings** → section divider → **Admin Resources** (Owner Pilot Runbook, Scoring Weight Model, Route Map, Scout Market Map, Deal Readiness Report; admin only)  
**Left nav:** **Support** — Help Center only

| Route | Content | API |
|-------|---------|-----|
| `/support` | Help hub | — |
| `/support/scoring-weight-model` | Operator + brand match weights (task 2.02) | `GET /api/support/scoring-weight-model` |
| `/support/owner-pilot-provisioning` | Owner pilot provisioning runbook | `GET /api/support/owner-pilot-provisioning-runbook` |

**GTM pilot copy** (warm intro, reply playbook, etc.) lives in **Google Drive Word files** and repo `docs/gtm-resources/` — not on the platform.

**Auth:** `memberstackAuth` → `requireDealalityUser` → `requireInternalRunbookAdmin` (same as existing runbook).

**Client gate:** `public/js/support-admin-gate.js` — blocks direct URL access for non-admins.

---

## Architecture pattern

```
docs/gtm-resources/*.md          lib/support/*-runbook.js
lib/*-scoring-weight-config.js         ↓
                                 api/support-*.js
                                       ↓
                            admin API (JSON runbook shape)
                                       ↓
                     public/app/support/*.html + owner-pilot-runbook.js
```

Adding a new internal doc:

1. Add markdown or config in `lib/` / `docs/`.
2. Create `lib/support/{name}-runbook.js` returning runbook JSON.
3. Add `api/support-{name}.js` handler + `server.js` route with `internalRunbookAuth`.
4. Add `public/app/support/{name}.html` + route in `public/app.js` (`internalRunbookOnly: true`).
5. Add card on `public/app/support/index.html` and nav entry under Support.

---

## What not to put on the platform

- PII from Pilot Target List
- Memberstack secrets or API keys
- Full Airtable record dumps

Word files on Drive remain the **primary outreach copy**; the platform mirrors markdown for quick admin lookup.

---

## Related repo docs

- [operator-alignment-scoring-weight-model.md](./operator-alignment-scoring-weight-model.md)
- [gtm-resources/README.md](./gtm-resources/README.md)
- [operator-alignment-field-matrix.md](./operator-alignment-field-matrix.md)
