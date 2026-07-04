# Operator Deal Requests — Phase 2 scoping

Approved scoped model for **My Operator Deals**. Phase 2 delivers secure, Airtable-backed reads/writes with server-enforced operator company scope — not the unauthenticated Brand Deal Requests pattern.

## Identity chain

```
Memberstack JWT → Users row → Operator Setup - Master (link) → company_name → Operating Company Name filter
```

| Step | Source |
|------|--------|
| Auth | `memberstackAuth` + `requireDealalityUser` + `requireOperatorDealsAccess` |
| Users link field | **`Operator Setup - Master`** (not legacy Profile & Positioning) |
| Company name | `company_name` on Operator Setup - Master |
| Active filter | `submission_status` ∈ env `AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES` (default `Active`) |
| ODR filter field | **`Operating Company Name`** on Operator Deal Requests |

## `/api/me` permissions (operators)

```json
{
  "permissions": {
    "allowedOperatingCompanyNames": ["Acme Hotel Operators"],
    "allowedOperatorSetupIds": ["recXXX"],
    "primaryOperatingCompanyName": "Acme Hotel Operators"
  },
  "meta": {
    "operatorMappingStatus": "ok",
    "operatorSetupLinkField": "Operator Setup - Master"
  }
}
```

**Mapping statuses:** `ok` | `no_operator_link` | `names_unresolved` | `lookup_error` | `admin_unrestricted`

## Multi-company operators

- Users may link to **multiple** Operator Setup - Master records.
- **One** active company → auto-select; no dropdown required.
- **Multiple** active companies → UI dropdown populated **only** from `/api/me` allow-list.
- Non-admin users **cannot** free-text filter; tampered `?operator=` outside allow-list → empty result set.

## API routes (all require auth stack)

| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/operator-deal-requests` | Scoped list; `?all=1` **admin-only** |
| GET | `/api/operator-deal-requests/:requestId` | Row-level scope check |
| GET | `/api/operator-deal-requests/activity` | Uses `Operating Company Name` on Deal Activity Log |
| PATCH | `/api/operator-deal-requests/:requestId` | Status, notes, follow-up only |
| POST | `/api/operator-deal-requests/bulk-update` | All rows verified before write; any out-of-scope → **403**, no partial updates |

**403 roles:** owner, brand (via `requireOperatorDealsAccess`).

## Activity log (Phase 2)

- Field: **`Operating Company Name`** (not generic Counterparty Name).
- **`Stakeholder`** includes **`Operator`**.

## Out of scope (Phase 2)

- Operator Deal Room
- Proposal submission
- Owner create path
- Full CRM expansion
- Partial bulk-update success

## Modules

| Module | Purpose |
|--------|---------|
| `lib/dealality/resolve-operator-scope.js` | Scope resolution + row access asserts |
| `api/operator-deal-requests.js` | Scoped CRUD + activity |
| `api/operator-deal-requests-fields.js` | `MAP_ODR_AIRTABLE` |
| `api/me.js` | Operator permissions on `/api/me` |
| `middleware/requireOperatorDealsAccess.js` | operator + admin gate |
| `public/operator-development-dashboard.js` | `/api/me` first, company dropdown, auth fetch |

## Env vars

See `.env.example`:

- `AIRTABLE_ME_USERS_OPERATOR_SETUP_LINK`
- `AIRTABLE_OPERATOR_SETUP_MASTER_TABLE`
- `AIRTABLE_OPERATOR_COMPANY_NAME_FIELD`
- `AIRTABLE_OPERATOR_SETUP_SUBMISSION_STATUS_FIELD`
- `AIRTABLE_OPERATOR_SETUP_ACTIVE_STATUS_VALUES`
- `AIRTABLE_TABLE_OPERATOR_DEAL_REQUESTS`

## Security tests

Run: `node scripts/validate-operator-deal-requests-scoping.mjs`

Checks S1–S12: auth middleware on all routes, no unscoped non-admin list, bulk 403 behavior, scope helper exports, `/api/me` operator fields, UI auth + company filter.

## Rollback

Revert `server.js` operator routes to Phase 1 stub imports; dashboard falls back to empty states if table missing (503).
