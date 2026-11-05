# Exact commands — run only after founder GO clears BLOCKED items

## A. Scoped commit (local; no production)

Stage **only** the deploy allowlist. Do **not** `git add -A`.

```bash
# Example: add allowlisted paths explicitly, then commit on PR branch
git add \
  lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js \
  lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js \
  api/group-demand-intelligence.js \
  scripts/test-gdi-one-click-hotel-publish-v1.mjs \
  scripts/gdi-one-click-publish-spice-island-dry-run-v1.mjs \
  reports/gdi-one-click-publication-architecture-v1/

# For server.js / app.js / package.json: stage surgically (patch) so unrelated churn is excluded.
# Then:
npm run test:gdi-one-click-hotel-publish-v1
npm run gdi:one-click-publish-spice-dry-run
git push -u origin HEAD
```

Confirm PR #136 CI green on the **new** head oid.

## B. Production backup (before any Spice write)

```bash
# On production host / volume mount — copy GDI published inventory root
# (path = GDI_PUBLISHED_INVENTORY_ROOT or volume subpath used for GDI)
# Destination example:
# reports/gdi-one-click-publication-architecture-v1/backup-volume-BEFORE_SPICE_ONE_CLICK_<UTC>/

# Must include:
#   registry/current-registry-v1.json
#   snapshots/recKRJjcPnb4tVDDS/**
#   history/recKRJjcPnb4tVDDS/**
# Do not modify ADP published trees under the same volume parent.
```

## C. Deploy

Merge PR #136 **or** pin Railway deploy source to the scoped one-click head.
Confirm deploy revision == that commit.

## D. Spice dry-run (production API; no advance)

```bash
curl -sS -X POST "$BASE/api/group-demand-intelligence/hotels/recKRJjcPnb4tVDDS/publish" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dryRun": true,
    "opportunityIds": [
      "gdi_s4912_ba7b1cb8a3ab",
      "gdi_s4912_b956aa67a4a2",
      "gdi_s4912_b6d9b1929c05",
      "gdi_s4912_cb2d71ab1b15",
      "gdi_s4912_cc2a8da6ca6e",
      "gdi_s4912_03deda917778",
      "gdi_s4912_3cafec77a21f"
    ]
  }'
```

Expect: `ok=true`, count 7, SALES_READY=1, ACCOUNT_DEVELOPMENT=6.

## E. Spice apply (FOUNDER GO only)

```bash
curl -sS -X POST "$BASE/api/group-demand-intelligence/hotels/recKRJjcPnb4tVDDS/publish" \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "dryRun": false,
    "opportunityIds": [
      "gdi_s4912_ba7b1cb8a3ab",
      "gdi_s4912_b956aa67a4a2",
      "gdi_s4912_b6d9b1929c05",
      "gdi_s4912_cb2d71ab1b15",
      "gdi_s4912_cc2a8da6ca6e",
      "gdi_s4912_03deda917778",
      "gdi_s4912_3cafec77a21f"
    ],
    "revokeMissingPublishedIds": true,
    "approval": {
      "authorized": true,
      "authorizationPhrase": "FOUNDER_GO_SPICE_ONE_CLICK_PUBLISH_V1"
    }
  }'
```

## F. Verify peers + share

- Spice share: 7 full-content IDs, token unchanged.
- Bethesda `recLuxvwwxID7U2B8`: still 30.
- Waterstone `recgMYovrrZDJMqzX`: still 28.

## G. Rollback

Restore `registry/current-registry-v1.json` (+ Spice snapshot/history trees if needed) from the backup taken in B.  
Share token file untouched. ADP trees untouched.
