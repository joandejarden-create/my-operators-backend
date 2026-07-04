# Users table consolidation (Option A)

Platform accounts, Memberstack, deals, company admin, and Partner Directory individuals all use the **Users** table (`tbl6shiyz2wdUqE5F`). The legacy **User Management** table is retired in application code after data migration.

## 1. Add fields on Users

**Automated (recommended):**

```bash
node scripts/ensure-users-platform-fields.mjs
```

Requires `AIRTABLE_API_KEY` with `schema.bases:write`. Copies single-select options from User Management when available.

**Manual:** create any missing columns on **Users** (same names as User Management where possible):

| Field | Type |
|-------|------|
| Company Title | Single line text |
| Phone Number | Phone |
| Company Email | Email (optional if you only use **Email**) |
| Platform Role | Single select |
| Contact Visibility | Single select |
| Deal Access | Single select |
| Document Access | Single select |
| Based (Country) | Single line text |
| Closed Deals | Number |
| Unique Brands (Deals) | Number |
| Submitted Bids | Number |
| Coverage Territories | Long text |
| Region - America | Checkbox |
| Region - Caribbean & Latin America | Checkbox |
| Region - Europe | Checkbox |
| Region - Middle East & Africa | Checkbox |
| Region - Asia Pacific | Checkbox |
| Profile (or Profile Picture / Headshot) | Attachment |

**Company link:** use **Company Profile** (linked to Company Profile table). Do not rely on a field named `Company` on Users.

**Memberstack (keep as-is):** Email, Unique Webflow ID / Slug, Platform Role or User Type.

## 2. Audit

```bash
node scripts/audit-users-vs-user-management.mjs
```

Fix any “Fields to add on Users” reported missing.

## 3. Migrate data

```bash
node scripts/migrate-user-management-to-users.mjs --dry-run
node scripts/migrate-user-management-to-users.mjs --apply
```

Output: `scripts/output/um-to-users-id-map.json` (old UM record id → Users record id).

## 3b. Full field backfill (Languages, responsiveness, Brands Supported, etc.)

After `ensure-users-platform-fields.mjs` adds any columns that only existed on User Management:

```bash
node scripts/backfill-users-from-user-management.mjs --dry-run
node scripts/backfill-users-from-user-management.mjs --apply --overwrite
```

`--overwrite` copies every UM value onto the mapped Users row (recommended once after adding fields).

## 4. Company Profile team links (required for Partner Directory teams)

The old **User Management** column on Company Profile only accepts UM record ids. The app reads **Users** ids.

```bash
node scripts/ensure-company-profile-team-members-field.mjs
node scripts/relink-company-profile-teams-to-users.mjs --apply
```

This creates **Team Members** (→ Users) and copies mapped team links from **User Management**.

## 5. Other Airtable links

- **User Favorites** → **Individual Profile** → prefer **User Profile** (Users) for new favorites.
- Any Webflow or Airtable automations that created UM rows → point at Users.

## 5. Deploy code

This repo already reads/writes **Users** for:

- `/api/me`, Memberstack auth, My Deals
- `/api/user-management`
- Partner Directory individuals (`/api/partner-directory`)

Redeploy Railway after pull.

## 6. Archive legacy table

When QA passes, hide or rename **User Management** in Airtable (do not delete until backups and link updates are done).

## Environment

| Variable | Default |
|----------|---------|
| `USERS_TABLE_ID` | `tbl6shiyz2wdUqE5F` |
| `LEGACY_USER_MANAGEMENT_TABLE_ID` | `tblQEpYKf2aYNKKjw` (migration scripts only) |
| `AIRTABLE_USERS_COMPANY_LINK_FIELD` | `Company Profile` |
