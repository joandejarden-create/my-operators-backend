# Acquisition Intelligence (internal)

**Status:** Stage 1 — LinkedIn Connections CSV ingestion + data foundation  
**Base:** `AIRTABLE_GTM_BASE_ID` (same GTM base as Owner Targets / Contacts / Decision Radar)  
**UI:** `/internal/acquisition-intelligence.html` (admin-gated)  
**SoT modules:** `lib/acquisition-intelligence/*`

---

## Purpose

Help Joan (and later authorized users) answer:

> Who in my existing professional network is most likely to help me acquire qualified hotel owners with live or imminent hotel decisions?

Stage 1 only: **upload → preview → import** private network relationships. No classification engine, deep research, scoring, or outreach.

---

## Relationship to Decision Radar

| System | Starts from | Job |
|--------|-------------|-----|
| **Decision Radar** | Project → Owner → Decision → warm path | Live owner decisions |
| **Acquisition Intelligence** | Person user knows → company/role → opportunities | Network → owner access |

They meet later at **Decision Opportunity** (do not duplicate). Stage 1 does not write Decision Opportunities.

---

## Architecture (hybrid)

```text
Authenticated user (memberstack / Dealality user id)
  → Acquisition Import Batch (audit)
  → Acquisition Network Relationship (user-scoped)
  → Contacts (global person identity)
```

- **Contacts** — reuse GTM Contacts; upsert by LinkedIn URL then name+company.
- **Acquisition Network Relationships** — `Source User Id` + Contact link; relationship strength / acquisition role live here (user-scoped).
- **Acquisition Import Batches** — per CSV upload audit trail.

Do **not** put Relationship Strength on global Contacts.

---

## Setup

```bash
node scripts/ensure-acquisition-intelligence-schema.mjs
node scripts/ensure-acquisition-intelligence-schema.mjs --apply
npm run test:acquisition-intelligence-linkedin-import
```

---

## CSV import behavior

1. Detect LinkedIn header row (metadata rows above header allowed).
2. Required columns: `First Name`, `Last Name`.
3. Preferred: `URL`, `Email Address`, `Company`, `Position`, `Connected On`.
4. Preview API returns stats; no writes until confirm.
5. Import is idempotent via `Relationship Dedupe Key` = `userId|li:url` or `userId|nc:name|company`.
6. Re-import updates LinkedIn-derived fields only; never overwrites blanks onto filled fields; never resets manual Relationship Strength / Notes.
7. Provenance: `Import Source = LINKEDIN_CONNECTIONS_EXPORT`.

---

## Security

- API: `memberstackAuth` + `requireDealalityUser` + `requireAdminAccess`.
- UI: `SupportAdminGate.requireAdmin`.
- User isolation: all relationship/batch queries filter by `Source User Id`.
- No LinkedIn scraping; URL is identity only.
- No automated outreach.
- Never commit real Connections.csv exports.

---

## Stage roadmap

| Stage | Scope |
|-------|--------|
| 1 | CSV ingestion + data model |
| 2 | Cheap deterministic classification (`acquisition-classify-v1`) |
| 3 | Network UI (table/filters/detail) |
| 4 | Deep research (selected only) |
| 5 | Direct Prospect + Connector scores |
| 6 | This Week founder priority |
| 7 | Prepare Outreach (no send) |
| 8 | Decision Radar linkage |

### Stage 2 classifier

- Config: `lib/acquisition-intelligence/classification-config.js`
- Engine: `lib/acquisition-intelligence/classify-relationship.js`
- Batch: `lib/acquisition-intelligence/classify-batch.js`
- Manual `Classification Source = Manual` blocks overwrite
- Relationship Strength remains Unknown / manual only
- API: `POST /api/acquisition-intelligence/classify`

---

## Data contract snapshot

| Item | Value |
|------|--------|
| Tables | `Acquisition Network Relationships`, `Acquisition Import Batches`, reuse `Contacts` |
| Mapping | `lib/acquisition-intelligence/field-map.js` |
| Required on relationship create | Relationship Name, Source User Id, Relationship Dedupe Key, Import Source |
| Select sources | `VAL_*` in field-map |
| Linked | Contact → Contacts; Import Batch → Acquisition Import Batches |
| Visibility | `internal_only` |
