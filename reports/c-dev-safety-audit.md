# C:\Dev read-only duplicate / overlap audit

Generated: 2026-10-01T19:20:14.7946331+02:00
Enriched: 2026-10-01T19:31:04.4183191+02:00
Primary repo: `C:\Dev\deal-capture-proxy` @ `caad4a37d1cd42d44ca0338f756d9cce0e77b30c` (`cursor/local-system-startup-recovery`)

**READ-ONLY** - no deletes, moves, resets, cleans, or repo mutations (except writing these report files).

## Executive summary

| Metric | Value |
|--------|-------|
| Top-level folders | 30 |
| Approx total size | **100.98 GB** |
| Active primary | `deal-capture-proxy` (dirty: recovery WIP) |
| Linked git worktrees | **15** (all under C:\Dev, DO NOT TOUCH / KEEP - WORKTREE) |
| Backup / snapshot roots | Backup-Staging (18.3 GB), Backup-Scripts (13.8 GB), dealality-backups (5.9 GB), Cursor-Recovery-Archive (5.1 GB), Nightly-Backups (3.5 GB) |
| True clean duplicates | **None** among non-empty folders |
| Empty dirs (deletion candidates) | `Dealality`, `deal-capture-proxy-enrich-dry-run`, `gdi-railway-up` (~0 GB) |
| Biggest disk consumers | Backup-Staging, Backup-Scripts (incl. snapshots), primary `node_modules`+`data`+`.git`, worktree working trees |

### Classification counts
- **B. ACTIVE GIT WORKTREE**: 15
- **G. BACKUP / SNAPSHOT**: 7
- **I. UNKNOWN - DO NOT TOUCH**: 4
- **H. GENERATED / CACHE**: 3
- **A. ACTIVE PRIMARY REPO**: 1

### Recommendation counts

- **KEEP - WORKTREE**: 15
- **KEEP - BACKUP**: 5
- **REQUIRES MANUAL REVIEW**: 4
- **SAFE TO CONSIDER DELETING**: 3
- **DO NOT TOUCH**: 2
- **KEEP - ACTIVE**: 1

### Critical DO NOT TOUCH (script / scheduler / worktree)

- `C:\Dev\deal-capture-proxy` - ACTIVE PRIMARY (dirty)
- All **15** linked worktrees listed below - removing them without `git worktree remove` damages the primary repo metadata
- `C:\Dev\Backup-Scripts` + `Backup-Dealality.ps1` - **Dealality Nightly Backup** scheduled task
- `C:\Dev\dealality-backups` - local LATEST backup target of Backup-Dealality.ps1
- `C:\Dev\Backup-Staging` - staging snapshots used in crash recovery (KEEP - BACKUP)

### Empty-folder deletion candidates only

| Folder | Why safe-looking | Duplicates? | Unique uncommitted? | Recoverable |
|--------|------------------|-------------|---------------------|-------------|
| Dealality | 0 files / 0 bytes | n/a | no | ~0 GB |
| deal-capture-proxy-enrich-dry-run | 0 files / 0 bytes | n/a | no | ~0 GB |
| gdi-railway-up | 0 files / 0 bytes | n/a | no | ~0 GB |

**No non-empty folder is recommended for deletion.** Deploy-* folders are live git worktrees, not disposable clones.

---
## Top-level inventory

| Folder | GB | Files | Git | Branch | HEAD | Dirty | M/U | Class | Recommendation |
|--------|----|-------|-----|--------|------|-------|-----|-------|----------------|
| Backup-Staging | 18.32 | 273719 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | KEEP - BACKUP |
| Backup-Scripts | 13.78 | 127734 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | DO NOT TOUCH |
| deal-capture-proxy | 11.02 | 218095 | Y | cursor/local-system-startup-recovery | caad4a37 | dirty | 35/17 | A. ACTIVE PRIMARY REPO | KEEP - ACTIVE |
| dealality-backups | 5.93 | 64740 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | DO NOT TOUCH |
| Cursor-Recovery-Archive-2026-09-08 | 5.14 | 805 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | KEEP - BACKUP |
| deal-capture-proxy-census | 3.76 | 68017 | Y WT | feature/census-cutover-2026-09-08 | 0f159137 | dirty | 89/2311 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| Nightly-Backups | 3.49 | 39285 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | KEEP - BACKUP |
| deal-capture-proxy-deploy-fd919a1 | 2.86 | 48065 | Y WT | HEAD | fd919a11 | dirty | 1/5 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-main-deploy | 2.83 | 46605 | Y WT | deploy/oi-hpc-cutover-p815-20260916 | 3fef5158 | dirty | 4/9 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-v92-deploy | 2.83 | 46575 | Y WT | HEAD | 1cc0e7dc | dirty | 0/3 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| dealality-fairfield | 2.83 | 46169 | Y WT | cursor/brand-explorer-fairfield-pilot-5554 | 78f35911 | clean | 0/0 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-hotfix-hi-share | 2.81 | 46573 | Y WT | hotfix/restore-mexico-hi-share-onto-main | b72148f4 | dirty | 0/6 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-v92-bpp-evidence | 2.81 | 45401 | Y WT | deploy/adp-v92-bpp-evidence-universal-20260911 | 92509fdb | dirty | 0/2 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| dealality-local-task-runner | 2.51 | 34201 | Y WT | feature/local-cursor-task-runner-issue-58 | ef8949e2 | dirty | 4/14 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-v91-release | 2.49 | 33887 | Y WT | deploy/adp-v91-bpp-rank-only-parity-20260910 | bf84b5c1 | dirty | 2/0 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-gdi-restore-20260915 | 2.49 | 34394 | Y WT | deploy/gdi-restore-coexist-20260915 | 5465352b | dirty | 6/115 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-deploy-cala-six | 2.45 | 33652 | Y WT | deploy/adp-cala-six-share-20260908 | 70b14df4 | clean | 0/0 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-adp-parity-deploy | 2.45 | 33663 | Y WT | deploy/adp-production-parity-current-published-20260909 | a503b1ce | clean | 0/0 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-adp-atomic-deploy | 2.45 | 33874 | Y WT | deploy/adp-atomic-customer-release-20260910 | 8ba68ece | dirty | 2/1 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-deploy-main | 2.45 | 33596 | Y WT | deploy/adp-client-share-ui-parity | 2bf2bc2c | clean | 0/0 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| deal-capture-proxy-deploy-monterrey | 2.45 | 33617 | Y WT | deploy/adp-monterrey-valle-share-20260908 | c2dc1db2 | dirty | 1/0 | B. ACTIVE GIT WORKTREE | KEEP - WORKTREE |
| data | 1.84 | 12 | N |  | - | clean | 0/0 | H. GENERATED / CACHE | REQUIRES MANUAL REVIEW |
| _gdi-deploy-parked-data | 0.30 | 7981 | N |  | - | clean | 0/0 | H. GENERATED / CACHE | REQUIRES MANUAL REVIEW |
| open-source | 0.25 | 2456 | N |  | - | clean | 0/0 | I. UNKNOWN - DO NOT TOUCH | REQUIRES MANUAL REVIEW |
| deal-capture-proxy-gdi-deploy-park | 0.24 | 5063 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | KEEP - BACKUP |
| gdi-v11-lean-deploy | 0.19 | 4904 | N |  | - | clean | 0/0 | G. BACKUP / SNAPSHOT | KEEP - BACKUP |
| Dealality | 0 | 0 | N |  | - | clean | 0/0 | I. UNKNOWN - empty dir | SAFE TO CONSIDER DELETING |
| deal-capture-proxy-enrich-dry-run | 0 | 0 | N |  | - | clean | 0/0 | I. UNKNOWN - empty dir | SAFE TO CONSIDER DELETING |
| fixtures | 0.00 | 4 | N |  | - | clean | 0/0 | H. GENERATED / CACHE | REQUIRES MANUAL REVIEW |
| gdi-railway-up | 0 | 0 | N |  | - | clean | 0/0 | I. UNKNOWN - empty dir | SAFE TO CONSIDER DELETING |

## Worktrees (from primary `git worktree list`)

- `C:/Dev/deal-capture-proxy` head=caad4a37d1cd42d44ca0338f756d9cce0e77b30c branch=refs/heads/cursor/local-system-startup-recovery
- `C:/Dev/deal-capture-proxy-adp-atomic-deploy` head=8ba68eceb35e5d5dfa7ff4a65825c6a2e6137fc7 branch=refs/heads/deploy/adp-atomic-customer-release-20260910
- `C:/Dev/deal-capture-proxy-adp-parity-deploy` head=a503b1ce507ffbf49b7ee8c8f706721f5d78d7de branch=refs/heads/deploy/adp-production-parity-current-published-20260909
- `C:/Dev/deal-capture-proxy-census` head=0f159137ccebf2fe81779643e230a8d2f3cbc042 branch=refs/heads/feature/census-cutover-2026-09-08
- `C:/Dev/deal-capture-proxy-deploy-cala-six` head=70b14df41c5ab9cfd8540a4dc280c4b9616b02b8 branch=refs/heads/deploy/adp-cala-six-share-20260908
- `C:/Dev/deal-capture-proxy-deploy-fd919a1` head=fd919a1135eeb0ef35fb04b3b038f4ec7afed88f branch=
- `C:/Dev/deal-capture-proxy-deploy-main` head=2bf2bc2c805c11df5af753dc423884fb2e9cd95e branch=refs/heads/deploy/adp-client-share-ui-parity
- `C:/Dev/deal-capture-proxy-deploy-monterrey` head=c2dc1db2f549ff262c68ddd80e6ab459926d104b branch=refs/heads/deploy/adp-monterrey-valle-share-20260908
- `C:/Dev/deal-capture-proxy-gdi-restore-20260915` head=5465352bba4fa02f4a5a45b08f8f2edd9548a269 branch=refs/heads/deploy/gdi-restore-coexist-20260915
- `C:/Dev/deal-capture-proxy-hotfix-hi-share` head=b72148f45e77bce243b3885d4c7158f1dcd59f3b branch=refs/heads/hotfix/restore-mexico-hi-share-onto-main
- `C:/Dev/deal-capture-proxy-main-deploy` head=3fef5158839eb427bbb673598b0046ac2f6af0b1 branch=refs/heads/deploy/oi-hpc-cutover-p815-20260916
- `C:/Dev/deal-capture-proxy-v91-release` head=bf84b5c1a4538591f399871e97c6d1e5873b17a7 branch=refs/heads/deploy/adp-v91-bpp-rank-only-parity-20260910
- `C:/Dev/deal-capture-proxy-v92-bpp-evidence` head=92509fdb92835a1c44b801eb62f621358c571979 branch=refs/heads/deploy/adp-v92-bpp-evidence-universal-20260911
- `C:/Dev/deal-capture-proxy-v92-deploy` head=1cc0e7dcfcbba18d2ef15dc4648c217dc811c75a branch=
- `C:/Dev/dealality-fairfield` head=78f359110d826e76a4959c1c66a80a275f71fd9f branch=refs/heads/cursor/brand-explorer-fairfield-pilot-5554
- `C:/Dev/dealality-local-task-runner` head=ef8949e26796f2c10963ccdebf50d02783f03f16 branch=refs/heads/feature/local-cursor-task-runner-issue-58

## Dealality file-level compare vs primary (api/lib/public/scripts/fixtures/config + package.json/server.js)

| Folder | Same HEAD | Ident | Differ | Only folder | Newer in folder | Dirty | Untracked | Unique commits | Class |
|--------|-----------|-------|--------|-------------|-----------------|-------|-----------|----------------|-------|
| deal-capture-proxy-adp-atomic-deploy | False | 32429 | 1445 | 0 | 0 | True | 1 | 1 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-adp-parity-deploy | False | 32121 | 1543 | 4 | 0 | False | 0 | 2 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-census | False | 32851 | 1219 | 5 | 0 | True | 2311 | 1 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-deploy-cala-six | False | 32109 | 1543 | 0 | 0 | False | 0 | 0 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-deploy-fd919a1 | False | 35361 | 294 | 0 | 0 | True | 5 | 0 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-deploy-main | False | 32038 | 1559 | 0 | 0 | False | 0 | 0 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-deploy-monterrey | False | 32066 | 1552 | 0 | 0 | True | 0 | 0 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-gdi-restore-20260915 | False | 33176 | 1104 | 0 | 0 | True | 115 | 0 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-hotfix-hi-share | False | 32970 | 1191 | 4 | 0 | True | 6 | 18 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-main-deploy | False | 32952 | 1236 | 26 | 0 | True | 9 | 36 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-v91-release | False | 32469 | 1419 | 0 | 0 | True | 0 | 3 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-v92-bpp-evidence | False | 32979 | 1188 | 7 | 0 | True | 2 | 20 | B. ACTIVE GIT WORKTREE |
| deal-capture-proxy-v92-deploy | False | 32979 | 1188 | 7 | 0 | True | 3 | 23 | B. ACTIVE GIT WORKTREE |
| dealality-fairfield | False | 32954 | 1259 | 42 | 0 | False | 0 | 47 | B. ACTIVE GIT WORKTREE |
| dealality-local-task-runner | False | 32952 | 1236 | 26 | 0 | True | 14 | 38 | B. ACTIVE GIT WORKTREE |

### Sample files only in clone / differing

**deal-capture-proxy-adp-atomic-deploy**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/golden-demo-ownership.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/scout-market-coverage.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json
- Only-in-folder sample: 

**deal-capture-proxy-adp-parity-deploy**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/golden-demo-ownership.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/scout-market-coverage.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json
- Only-in-folder sample: scripts/run-adp-bpp-full-universe-remediation-v1.mjs; scripts/run-adp-production-parity-current-published-audit-v1.mjs; scripts/test-adp-bpp-full-universe-client-ready-v1.mjs; scripts/test-adp-current-published-share-parity-v1.mjs

**deal-capture-proxy-census**
- Differ sample: api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json; config/client-share/production-share-contract-tokens.json; config/group-demand-intelligence/hotels/gdi_hotel_ac_hotel_a_coruna.json; config/group-demand-intelligence/hotels/gdi_hotel_renaissance_times_square.json; config/group-demand-intelligence/hotels/gdi_hotel_spice_island_beach_resort.json; config/group-demand-intelligence/hotels/gdi_hotel_w_rome.json; config/group-demand-intelligence/hotels/gdi_hotel_waterstone_boca_raton.json
- Only-in-folder sample: lib/hotel-census/brand-presence-hpc-adapter.js; lib/hotel-census/brand-presence-hpc-request.js; lib/hotel-census/product-safe-census-fields.js; scripts/test-brand-presence-hpc-adapter.mjs; scripts/test-brand-presence-hpc-routing-matrix.mjs

**deal-capture-proxy-deploy-cala-six**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/golden-demo-ownership.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/scout-market-coverage.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json
- Only-in-folder sample: 

**deal-capture-proxy-deploy-fd919a1**
- Differ sample: api/decision-outcomes.js; api/market-alerts-contact.js; fixtures/ai-demand-positioning/monthly-review/action-register-empty-v1.json; fixtures/ai-demand-positioning/monthly-review/cambridge-beaches-monthly-executive-review-v1.json; fixtures/ai-demand-positioning/monthly-review/now-now-noho-monthly-executive-review-v1.json; fixtures/hotel-intelligence/contact-intelligence/kgpv-contact-intelligence-showcase-v1.json; fixtures/hotel-intelligence/owner-portfolio/dle_06G6AB1VK0BCCD94DNN7W8DRWZ.json; fixtures/hotel-intelligence/owner-portfolio/ent_alliance-hotel-management.json; fixtures/hotel-intelligence/owner-portfolio/ent_dovetail-hospitality.json; fixtures/hotel-intelligence/owner-portfolio/ent_inmobiliaria-hnf.json; lib/context-dev/credit-ledger.js; lib/context-dev/index.js; lib/context-dev/operation-credit-bounds.js; lib/fullenrich/client.js; lib/fullenrich/index.js; lib/hotel-census/census-map-snapshot.js; lib/hotel-census/map-hotel-dto.js; lib/hotel-intelligence/contact-intelligence/THIRD_PARTY_NOTICES.ownership-iterative-research.md; lib/hotel-intelligence/contact-intelligence/actionable-metrics.js; lib/hotel-intelligence/contact-intelligence/adaptive-ownership-controller/candidate-quality.js; lib/hotel-intelligence/contact-intelligence/adaptive-ownership-controller/candidate-store.js; lib/hotel-intelligence/contact-intelligence/adaptive-ownership-controller/constants.js; lib/hotel-intelligence/contact-intelligence/adaptive-ownership-controller/controller.js; lib/hotel-intelligence/contact-intelligence/adaptive-ownership-controller/evaluate-candidates.js; lib/hotel-intelligence/contact-intelligence/adaptive-ownership-controller/index.js
- Only-in-folder sample: 

**deal-capture-proxy-deploy-main**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/golden-demo-ownership.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/scout-market-coverage.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json
- Only-in-folder sample: 

**deal-capture-proxy-deploy-monterrey**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/golden-demo-ownership.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/scout-market-coverage.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json
- Only-in-folder sample: 

**deal-capture-proxy-gdi-restore-20260915**
- Differ sample: api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/contact-intelligence.js; api/decision-outcomes.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json; config/client-share/production-share-contract-tokens.json; config/group-demand-intelligence/hotels/gdi_hotel_ac_hotel_a_coruna.json; config/group-demand-intelligence/hotels/gdi_hotel_renaissance_times_square.json; config/group-demand-intelligence/hotels/gdi_hotel_spice_island_beach_resort.json; config/group-demand-intelligence/hotels/gdi_hotel_w_rome.json; config/group-demand-intelligence/hotels/gdi_hotel_waterstone_boca_raton.json; config/group-demand-intelligence/hotels/rec2PVBDavppGpenm.json; config/group-demand-intelligence/hotels/rec35fExUxCClpOP6.json; config/group-demand-intelligence/hotels/rec8hHupaSwiWI3r7.json
- Only-in-folder sample: 

**deal-capture-proxy-hotfix-hi-share**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json; config/client-share/production-share-contract-tokens.json; config/group-demand-intelligence/hotels/gdi_hotel_ac_hotel_a_coruna.json; config/group-demand-intelligence/hotels/gdi_hotel_renaissance_times_square.json; config/group-demand-intelligence/hotels/gdi_hotel_spice_island_beach_resort.json
- Only-in-folder sample: fixtures/golden-demo/radar/area-hotels-bermuda.json; fixtures/golden-demo/radar/area-hotels-puerto-vallarta.json; fixtures/golden-demo/radar/hotel-recIwaP1etgx2g9nA.json; fixtures/golden-demo/radar/hotel-recUNycnMwOVFX0hc.json

**deal-capture-proxy-main-deploy**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-library.js; api/brand-presence.js; api/contact-intelligence.js; api/dealality-runtime-flags.js; api/decision-outcomes.js; api/demand-anchors.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/operator-census-footprint.js; api/operators-by-brand-region.js; api/owner-intelligence.js; api/scout-insight-review.js; api/scout-market-coverage.js; api/scout-market-insights.js; api/scout-market-map.js; api/scout-opportunity-signals.js; api/third-party-operator-detail.js; api/travel-infrastructure.js
- Only-in-folder sample: api/dealality-runtime-flags.js; config/hotel-census/legacy-to-hpc-id-map.json; fixtures/golden-demo/radar/area-hotels-bermuda.json; fixtures/golden-demo/radar/area-hotels-puerto-vallarta.json; fixtures/golden-demo/radar/hotel-recIwaP1etgx2g9nA.json; fixtures/golden-demo/radar/hotel-recUNycnMwOVFX0hc.json; lib/hotel-census/brand-explorer-hpc-metrics.js; lib/hotel-census/brand-presence-hpc-adapter.js; lib/hotel-census/brand-presence-hpc-request.js; lib/hotel-census/legacy-census-write-guard.js; lib/hotel-census/operator-explorer-hpc-metrics.js; lib/hotel-census/operator-intelligence-hpc.js; lib/hotel-census/product-safe-census-fields.js; lib/scout/scout-hpc-adapter.js; lib/scout/scout-hpc-request.js; public/js/brand-explorer-census-source.js; public/js/operator-explorer-census-source.js; public/js/radar-census-source.js; public/js/scout-census-source.js; scripts/hotel-census-v2-p810-be-shadow.mjs; scripts/hotel-census-v2-p810-write-reports.mjs; scripts/qa-adp-bpp-evidence-drawer-browser-v1.mjs; scripts/rebuild-adp-bpp-evidence-client-ready-v1.mjs; scripts/test-adp-bpp-evidence-client-ready.mjs; scripts/test-brand-presence-hpc-adapter.mjs

**deal-capture-proxy-v91-release**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/golden-demo-ownership.js; api/group-demand-intelligence.js; api/hotel-intelligence-dossier.js; api/hotel-intelligence-research.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/scout-market-coverage.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/bpp-customer-published-v1.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json
- Only-in-folder sample: 

**deal-capture-proxy-v92-bpp-evidence**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json; config/client-share/production-share-contract-tokens.json; config/group-demand-intelligence/hotels/gdi_hotel_ac_hotel_a_coruna.json; config/group-demand-intelligence/hotels/gdi_hotel_renaissance_times_square.json; config/group-demand-intelligence/hotels/gdi_hotel_spice_island_beach_resort.json; config/group-demand-intelligence/hotels/gdi_hotel_w_rome.json
- Only-in-folder sample: fixtures/golden-demo/radar/area-hotels-bermuda.json; fixtures/golden-demo/radar/area-hotels-puerto-vallarta.json; fixtures/golden-demo/radar/hotel-recIwaP1etgx2g9nA.json; fixtures/golden-demo/radar/hotel-recUNycnMwOVFX0hc.json; scripts/qa-adp-bpp-evidence-drawer-browser-v1.mjs; scripts/rebuild-adp-bpp-evidence-client-ready-v1.mjs; scripts/test-adp-bpp-evidence-client-ready.mjs

**deal-capture-proxy-v92-deploy**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-presence.js; api/contact-intelligence.js; api/decision-outcomes.js; api/demand-anchors.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/owner-intelligence.js; api/travel-infrastructure.js; api/webhooks-surfe-enrichment.js; config/client-share/adp-share-registry/active-tokens.json; config/client-share/bai-share-registry/active-tokens.json; config/client-share/gdi-share-registry/active-tokens.json; config/client-share/gdi-share-registry/revoked-token-ids.json; config/client-share/production-share-contract-tokens.json; config/group-demand-intelligence/hotels/gdi_hotel_ac_hotel_a_coruna.json; config/group-demand-intelligence/hotels/gdi_hotel_renaissance_times_square.json; config/group-demand-intelligence/hotels/gdi_hotel_spice_island_beach_resort.json; config/group-demand-intelligence/hotels/gdi_hotel_w_rome.json
- Only-in-folder sample: fixtures/golden-demo/radar/area-hotels-bermuda.json; fixtures/golden-demo/radar/area-hotels-puerto-vallarta.json; fixtures/golden-demo/radar/hotel-recIwaP1etgx2g9nA.json; fixtures/golden-demo/radar/hotel-recUNycnMwOVFX0hc.json; scripts/qa-adp-bpp-evidence-drawer-browser-v1.mjs; scripts/rebuild-adp-bpp-evidence-client-ready-v1.mjs; scripts/test-adp-bpp-evidence-client-ready.mjs

**dealality-fairfield**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-library.js; api/brand-presence.js; api/contact-intelligence.js; api/dealality-runtime-flags.js; api/decision-outcomes.js; api/demand-anchors.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/operator-census-footprint.js; api/operators-by-brand-region.js; api/owner-intelligence.js; api/scout-insight-review.js; api/scout-market-coverage.js; api/scout-market-insights.js; api/scout-market-map.js; api/scout-opportunity-signals.js; api/third-party-operator-detail.js; api/travel-infrastructure.js
- Only-in-folder sample: api/dealality-runtime-flags.js; config/hotel-census/legacy-to-hpc-id-map.json; fixtures/brand-explorer-presentation-fairfield-by-marriott-full.json; fixtures/golden-demo/radar/area-hotels-bermuda.json; fixtures/golden-demo/radar/area-hotels-puerto-vallarta.json; fixtures/golden-demo/radar/hotel-recIwaP1etgx2g9nA.json; fixtures/golden-demo/radar/hotel-recUNycnMwOVFX0hc.json; lib/hotel-census/brand-explorer-hpc-metrics.js; lib/hotel-census/brand-presence-hpc-adapter.js; lib/hotel-census/brand-presence-hpc-request.js; lib/hotel-census/legacy-census-write-guard.js; lib/hotel-census/operator-explorer-hpc-metrics.js; lib/hotel-census/operator-intelligence-hpc.js; lib/hotel-census/product-safe-census-fields.js; lib/partner-intelligence/brand-explorer-brand-website-preference.js; lib/partner-intelligence/brand-explorer-export-wave16a-full-fixture.js; lib/partner-intelligence/brand-explorer-factory-preview-staging-overlay.js; lib/partner-intelligence/brand-explorer-fairfield-content-remediation.js; lib/partner-intelligence/brand-explorer-fairfield-openings-fixture-rows.js; lib/partner-intelligence/brand-explorer-pilot-content-quality.js; lib/partner-intelligence/brand-explorer-section-pattern-parity-content-fairfield.js; lib/scout/scout-hpc-adapter.js; lib/scout/scout-hpc-request.js; public/js/brand-explorer-census-source.js; public/js/operator-explorer-census-source.js

**dealality-local-task-runner**
- Differ sample: api/admin-adp-leak-audits.js; api/admin-adp-monthly-reviews.js; api/admin-external-client-links.js; api/admin-gdi-reports.js; api/ai-demand-positioning.js; api/brand-library.js; api/brand-presence.js; api/contact-intelligence.js; api/dealality-runtime-flags.js; api/decision-outcomes.js; api/demand-anchors.js; api/group-demand-intelligence.js; api/lib/market-alerts-intelligence-map.js; api/market-alerts-contact.js; api/me.js; api/operator-census-footprint.js; api/operators-by-brand-region.js; api/owner-intelligence.js; api/scout-insight-review.js; api/scout-market-coverage.js; api/scout-market-insights.js; api/scout-market-map.js; api/scout-opportunity-signals.js; api/third-party-operator-detail.js; api/travel-infrastructure.js
- Only-in-folder sample: api/dealality-runtime-flags.js; config/hotel-census/legacy-to-hpc-id-map.json; fixtures/golden-demo/radar/area-hotels-bermuda.json; fixtures/golden-demo/radar/area-hotels-puerto-vallarta.json; fixtures/golden-demo/radar/hotel-recIwaP1etgx2g9nA.json; fixtures/golden-demo/radar/hotel-recUNycnMwOVFX0hc.json; lib/hotel-census/brand-explorer-hpc-metrics.js; lib/hotel-census/brand-presence-hpc-adapter.js; lib/hotel-census/brand-presence-hpc-request.js; lib/hotel-census/legacy-census-write-guard.js; lib/hotel-census/operator-explorer-hpc-metrics.js; lib/hotel-census/operator-intelligence-hpc.js; lib/hotel-census/product-safe-census-fields.js; lib/scout/scout-hpc-adapter.js; lib/scout/scout-hpc-request.js; public/js/brand-explorer-census-source.js; public/js/operator-explorer-census-source.js; public/js/radar-census-source.js; public/js/scout-census-source.js; scripts/hotel-census-v2-p810-be-shadow.mjs; scripts/hotel-census-v2-p810-write-reports.mjs; scripts/qa-adp-bpp-evidence-drawer-browser-v1.mjs; scripts/rebuild-adp-bpp-evidence-client-ready-v1.mjs; scripts/test-adp-bpp-evidence-client-ready.mjs; scripts/test-brand-presence-hpc-adapter.mjs

## Hard-coded C:\\Dev references

Hit count: 0

Top-level folders referenced: 


## Scheduled tasks (Dealality/Backup/deal)

- **Dealality Nightly Backup** (Ready): `powershell.exe -NoProfile -ExecutionPolicy Bypass -File "C:\Dev\Backup-Scripts\Backup-Dealality.ps1"`
- **MareBackup** (Ready): `%windir%\system32\compattelrunner.exe -m:aeinv.dll -f:UpdateSoftwareInventoryW invsvc | %windir%\system32\compattelrunner.exe -m:appraiser.dll -f:DoScheduledTelemetryRun | %windir%\system32\compattelrunner.exe -m:aemarebackup.dll -f:BackupMareData`
- **Backup** (Ready): ` `
- **BackupNonMaintenance** (Ready): ` `
- **Backup** (Ready): ` `
- **RegIdleBackup** (Ready): ` `

## Disk breakdown (largest folders)

### Backup-Staging (18.32 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 0 GB
- public: 0 GB
- reports: 0 GB
- fixtures: 0 GB
- api+lib+scripts: 0 GB
- Class: G. BACKUP / SNAPSHOT
- Recommendation: KEEP - BACKUP
- Evidence: backup/staging/snapshot

### Backup-Scripts (13.78 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 0 GB
- public: 0 GB
- reports: 0 GB
- fixtures: 0 GB
- api+lib+scripts: 0 GB
- Class: G. BACKUP / SNAPSHOT
- Recommendation: KEEP - BACKUP
- Evidence: backup/staging/snapshot

### deal-capture-proxy (11.02 GB)
- node_modules: 0.32 GB
- .git: 4.00 GB
- data: 1.96 GB
- public: 0.09 GB
- reports: 1.04 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.06 GB
- Class: A. ACTIVE PRIMARY REPO
- Recommendation: KEEP - ACTIVE
- Evidence: primary working repo

### dealality-backups (5.93 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 0 GB
- public: 0 GB
- reports: 0 GB
- fixtures: 0 GB
- api+lib+scripts: 0 GB
- Class: G. BACKUP / SNAPSHOT
- Recommendation: KEEP - BACKUP
- Evidence: backup/staging/snapshot

### Cursor-Recovery-Archive-2026-09-08 (5.14 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 0 GB
- public: 0 GB
- reports: 0 GB
- fixtures: 0 GB
- api+lib+scripts: 0 GB
- Class: G. BACKUP / SNAPSHOT
- Recommendation: KEEP - BACKUP
- Evidence: backup/staging/snapshot

### deal-capture-proxy-census (3.76 GB)
- node_modules: 0.36 GB
- .git: 0 GB
- data: 1.48 GB
- public: 0.09 GB
- reports: 0.95 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.06 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### Nightly-Backups (3.49 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 0 GB
- public: 0 GB
- reports: 0 GB
- fixtures: 0 GB
- api+lib+scripts: 0 GB
- Class: G. BACKUP / SNAPSHOT
- Recommendation: KEEP - BACKUP
- Evidence: backup/staging/snapshot

### deal-capture-proxy-deploy-fd919a1 (2.86 GB)
- node_modules: 0.32 GB
- .git: 0 GB
- data: 1.07 GB
- public: 0.09 GB
- reports: 0.97 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.06 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### deal-capture-proxy-main-deploy (2.83 GB)
- node_modules: 0.32 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.96 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### deal-capture-proxy-v92-deploy (2.83 GB)
- node_modules: 0.32 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.96 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### dealality-fairfield (2.83 GB)
- node_modules: 0.32 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.96 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### deal-capture-proxy-hotfix-hi-share (2.81 GB)
- node_modules: 0.32 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.95 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### deal-capture-proxy-v92-bpp-evidence (2.81 GB)
- node_modules: 0.31 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.96 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### dealality-local-task-runner (2.51 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.96 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

### deal-capture-proxy-v91-release (2.49 GB)
- node_modules: 0 GB
- .git: 0 GB
- data: 1.06 GB
- public: 0.09 GB
- reports: 0.95 GB
- fixtures: 0.05 GB
- api+lib+scripts: 0.05 GB
- Class: B. ACTIVE GIT WORKTREE
- Recommendation: KEEP - WORKTREE
- Evidence: active git worktree

## DO NOT TOUCH list

- **Backup-Scripts**: DO NOT TOUCH - Referenced by Backup-Dealality.ps1 and Dealality Nightly Backup scheduled task
- **Backup-Staging**: KEEP - BACKUP - backup/staging/snapshot
- **Cursor-Recovery-Archive-2026-09-08**: KEEP - BACKUP - backup/staging/snapshot
- **data**: REQUIRES MANUAL REVIEW - loose data/fixtures/cache at C:\Dev root
- **deal-capture-proxy**: KEEP - ACTIVE - primary working repo
- **deal-capture-proxy-adp-atomic-deploy**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-adp-parity-deploy**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-census**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-deploy-cala-six**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-deploy-fd919a1**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-deploy-main**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-deploy-monterrey**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-gdi-deploy-park**: KEEP - BACKUP - backup/staging/snapshot
- **deal-capture-proxy-gdi-restore-20260915**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-hotfix-hi-share**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-main-deploy**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-v91-release**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-v92-bpp-evidence**: KEEP - WORKTREE - active git worktree
- **deal-capture-proxy-v92-deploy**: KEEP - WORKTREE - active git worktree
- **dealality-backups**: DO NOT TOUCH - Referenced by Backup-Dealality.ps1 and Dealality Nightly Backup scheduled task
- **dealality-fairfield**: KEEP - WORKTREE - active git worktree
- **dealality-local-task-runner**: KEEP - WORKTREE - active git worktree
- **fixtures**: REQUIRES MANUAL REVIEW - loose data/fixtures/cache at C:\Dev root
- **gdi-v11-lean-deploy**: KEEP - BACKUP - backup/staging/snapshot
- **Nightly-Backups**: KEEP - BACKUP - backup/staging/snapshot
- **open-source**: REQUIRES MANUAL REVIEW - insufficient evidence for safe deletion
- **_gdi-deploy-parked-data**: REQUIRES MANUAL REVIEW - loose data/fixtures/cache at C:\Dev root

## Deletion / move candidates (manual confirmation required - NO ACTION TAKEN)

### Empty directories only (SAFE TO CONSIDER DELETING)

#### Dealality
- Path: `C:\Dev\Dealality`
- Size: 0 GB / 0 files
- Why: empty directory; not a git repo
- Unique uncommitted/untracked: none
- Recoverable space: negligible

#### deal-capture-proxy-enrich-dry-run
- Path: `C:\Dev\deal-capture-proxy-enrich-dry-run`
- Size: 0 GB / 0 files
- Why: empty directory; not a git repo / not a registered worktree
- Unique uncommitted/untracked: none
- Recoverable space: negligible

#### gdi-railway-up
- Path: `C:\Dev\gdi-railway-up`
- Size: 0 GB / 0 files
- Why: empty directory; not a git repo
- Unique uncommitted/untracked: none
- Recoverable space: negligible

### Non-empty folders

**None** met SAFE TO CONSIDER DELETING. All `deal-capture-proxy-*` git folders are **linked worktrees** of the primary repo (see `git worktree list`). Removing them as ordinary folders is unsafe.

`gdi-v11-lean-deploy` (0.19 GB) and `deal-capture-proxy-gdi-deploy-park` (0.24 GB) are non-git parked trees - **REQUIRES MANUAL REVIEW / KEEP - BACKUP** until contents are confirmed disposable.

`data`, `fixtures`, `_gdi-deploy-parked-data`, `open-source` - **REQUIRES MANUAL REVIEW**.

---

READY FOR JOAN REVIEW - C:\\DEV READ-ONLY DUPLICATE AUDIT COMPLETE

