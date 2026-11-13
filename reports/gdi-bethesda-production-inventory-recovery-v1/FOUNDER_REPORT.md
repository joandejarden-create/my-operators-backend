# Bethesda Marriott — Production Inventory Recovery (stop for publication approval)

**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**As of:** 2026-11-13  
**Production URL:** https://my-operators-backend-production.up.railway.app  
**PR #235:** still unmerged (guided-tour drawer Next/Back) — **not** included in this recovery.

## 1. Root cause

Production is **not** stuck on a Sep-13-only filesystem snapshot as the primary explanation.

| Surface | What it loads | Observed count |
|---|---|---:|
| Local / prod Airtable canonical store | `loadOpportunitiesCanonical` | 59 bodies |
| Bethesda internal research union | store ∪ reconstructions | 88 |
| **Sales-team inventory** (auth commercial + capability) | frozen baseline IDs via `selectBethesdaSalesOpportunityInventory` | **67** → Market Scope **All 67 / Hotel 61 / Competitive 10** |
| **Customer-facing heuristic** (`filterCustomerFacingOpportunities`) | live `customerVisible` / readiness rules | **27** |
| **Production external share (live today)** | share API uses customerFacing heuristic; **no** population annotation | **27** (UI shows 26 of 27) |

Exact share bug in `getGdiShareOpportunities`:

1. Uses `filterCustomerFacingOpportunities` instead of the frozen published-19 / approved-67 selector → **inflates to 27** (19 published + 7 staged leaks + 1 non-baseline).
2. Never calls `annotateInventoryPopulations` → Market Scope **Competitive (0)** even though CMI rows are present.

**Airtable already holds the bodies needed for the 67 baseline** (sales select: 67 selected, 0 missing). This is an **audience + share-path** problem, not a missing Airtable upsert of 40 rows.

## 2. Canonical inventory source

| Purpose | Canonical source |
|---|---|
| Sales-team working inventory | Frozen **67-ID** baseline (`bethesda-67-sales-baseline-cmi-v1`) over Bethesda union |
| Customer / external share (unapproved) | Frozen **published 19** |
| Customer / external share (approved) | Same **67** after founder publication receipt |
| Persistence | Airtable primary (`persistence: airtable`) with local FS mirror |

Approval gate (required for share → 67):

- File: `data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/bethesda-67-customer-publication-approval.json` with `approved: true`, **or**
- Env: `GDI_BETHESDA_67_CUSTOMER_PUBLICATION_APPROVED=1`

**Receipt does not exist today.** Env flag is not set. Expanding the client share to 67 without that receipt would bypass the product publication gate.

## 3. Reconciliation summary (IDs)

Artifacts:

- `00-prod-share-opportunities.json` — live production share capture (27)
- `01-reconciliation-summary.json`
- `02-record-level-reconciliation.json`
- `03-staged48-missing-from-prod-share.json`
- `04-published19-missing-from-prod-share.json`
- `backup-local-feedback.json`

| Bucket | Count | Meaning |
|---|---:|---|
| Production share IDs | 27 | Live client feed |
| Of which in published-19 | 19 | Already authorized |
| Of which in staged-48 | 7 | Leaked via customerVisible heuristics |
| Outside baseline-67 | 1 | `gdi_opp_2027_nci_rna_biology_symposium_7` |
| Staged-48 not on share | 41 | Require publication approval for share |
| UI “26 of 27” | — | Browse/filter exclusion on one API row — not a second inventory |

**26 vs 27:** one of the 27 API opportunities is hidden by the active weekly/priority/type filter chip state (“Showing 26 of 27”), not missing from the API.

## 4. Feedback preservation

- Local feedback backup: `backup-local-feedback.json` (from `data/.../feedback.json`).
- Production share feedback route is not exposed as a public GET; share validation overlays travel on opportunity DTOs (`shareValidation`).
- **No production feedback writes** were performed.
- Identity mapping remains opportunity ID — safe to keep once share selection switches to frozen IDs.

## 5. Competitive intelligence

| Mode | Competitive population (inventory counts) |
|---|---|
| Production share today | **0** (annotation missing on share DTO) |
| Unapproved share after code fix | 19 rows; Competitive population **1** (historical) |
| **Approved share (simulated)** | **All 67 / Hotel 61 / Competitive 10** (6 verified / 3 needs verification / 1 historical) |

Verified CMI is preserved in the baseline ledger; booked-competitor posture is not rewritten as open hotel RFPs by this work.

## 6. Code prepared (not deployed)

| Change | Purpose |
|---|---|
| `lib/.../bethesda-share-inventory-v1.js` | Union → commercial surface → frozen select → annotate |
| `api/group-demand-intelligence.js` share list + CSV | Wire helper for Bethesda; emit `listScope` + `inventoryPopulationCounts` |
| `scripts/test-gdi-bethesda-share-inventory-v1.mjs` | PASS: unapproved=19, approved=67, populations present |
| `scripts/gdi-bethesda-production-inventory-recovery-audit-v1.mjs` | Read-only prod vs local audit |

**Local simulation after fix:**

- `forceApproved: false` → **19** (`BETHESDA_PUBLISHED_CUSTOMER_FEED_19`)
- `forceApproved: true` → **67 / 61 / 10** (`BETHESDA_SALES_TEAM_INVENTORY_67`)

## 7. What was NOT done (by design)

- No approval receipt written.
- No Airtable mass upsert / no count forcing / no `customerVisible` bulk flips.
- No Railway deploy.
- No merge of PR #235.
- No claim that production already shows 67.

## 8. Founder decision required

Choose one:

**A — Authorize customer publication of the Bethesda 67 baseline**  
Then: write the approval receipt (or set the env flag on Railway), merge the share-path fix, deploy via GHA production release, re-verify the existing Bethesda share URL → expect **All 67 / Hotel 61 / Competitive 10**.

**B — Keep external share on the frozen published feed**  
Then: merge/deploy the share-path fix only → production share moves **27 → 19** (corrects the heuristic leak) with Market Scope annotation; sales-team auth UI continues to show 67.

**C — Hybrid**  
Keep share at 19; use sales-team authenticated GDI for the 67 working inventory; approve selected staged IDs later in batches.

## 9. Existing external client link (still valid; still 27 until deploy+approval)

https://my-operators-backend-production.up.railway.app/group-demand-intelligence-share.html?share=gdishare.v1.eyJ2IjoxLCJ0aWQiOiJnZGlzaHRfNDdjMjVkNzRjNzkyMTYwMjFmYjM2MTUwIiwiaG90ZWxJZCI6InJlY0x1eHZ3d3hJRDdVMkI4Iiwic3VyZmFjZXMiOlsiYnJpZWYiLCJvcHBvcnR1bml0aWVzIiwib3Bwb3J0dW5pdHlfZGV0YWlsIiwic3VtbWFyeSJdLCJtb2RlIjoicmVhZF9vbmx5IiwiaWF0IjoxNzg5MjE0OTMyLCJleHAiOjE3OTg2NzUyMDB9.DcmVDLtAQD_XkAvgI8mn6ir8YREmAwJ0uG3VOQXY-gc

Token `gdisht_47c25d74c79216021fb36150` remains ACTIVE. **Not client-ready for the 67 baseline until decision A + deploy.**

## 10. Outstanding risks

- Deploying the share fix **without** approval shrinks the public feed 27→19 (correct vs frozen publication, but a visible change for anyone using the current 27).
- Seven staged IDs currently on the live share via heuristic leak will disappear until explicitly approved.
- PR #235 tour drawer Next/Back remains broken on production/main.
- `/health` `buildSha` still reflects Railway git metadata, not upload SHA — use HTML/asset markers for deploy verification.
