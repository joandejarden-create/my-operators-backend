# Phase 1 final main reconciliation — PR #364

**STOP FOR FOUNDER REVIEW — not merged, not deployed.**

| Item | Value |
|------|-------|
| Branch | `cursor/adp-hotel-explorer-audit-1ed2` |
| Prior tip | `83b1ceb2a` |
| Reconciled tip | `a373fa579` |
| Merge | `merge(origin/main): reconcile PR #364 Hotel Explorer with latest GDI main` |
| Strategy | Normal merge (ort); **no** rebase / force-push |

## 1. Protection

- Pre-merge HEAD confirmed `83b1ceb2a`.
- Local 1.3B artifacts verified on disk; `package.json` 1.3B script overlay backed up to `preserve-local-13b/` then restored to committed HE-only `package.json` for clean merge.
- No `git clean` / `reset --hard`.
- Primary GDI checkout untouched; port 8080 still responding.

## 2. Main commits incorporated

| SHA | Title |
|-----|-------|
| `5dc2650d0` | CD-1–CD-8 commercial data-model durability (#366) |
| `123453323` | Bethesda four-record Airtable production pilot (#367) |
| `2fd417f5c` | GDI multi-hotel Airtable reconciliation V2 (#368) |
| `d898263bb` | All-hotel GDI research vs GDO persistence (#369) |
| `813204f61` | GDI research inventory table + YOTEL/Guadalajara backfill (#370) |
| `9632c5154` | Research inventory idempotency + all-hotel backfill (#371) |

## 3. Conflict resolution

- **`package.json`**: auto-merged cleanly — GDI scripts from main + ADP HE gates retained.
- No manual conflict markers.
- HE wiring (`deal-capture-radar-with-ranked-list.html`, `server.js` internal-preview route, auth latch) preserved.
- Main GDI modules/reports/scripts taken as-is (not overwritten by older ADP).

## 4. Workstream separation

**In PR #364:** Hotel Explorer Phase 1 + Bethesda authorized internal preview + closeout evidence.  
**Out (still untracked locally):** `data/hotel-identity/`, `lib/hotel-identity/`, `phase-1.3a/`, `phase-1.3b/`, 1.3B scripts. No census cutover.

## 5. Regression / security

| Gate | Result |
|------|--------|
| `test:adp-phase1-radar-he-gates` | PASS |
| Permanence Layer A | PASS |
| Layer B | BLOCKED (pre-existing) |
| `test:gdi-airtable-purpose-integrity-audit-v1` | PASS |
| `test:gdi-multi-hotel-airtable-reconciliation-v2` | PASS |
| Public brand-presence Bethesda | Absent (15692) |
| Internal-preview no auth / query-alone | **403** |
| Internal-preview + `X-Dealality-Owner-App: 1` | **200** + Bethesda |
| `DEV_AUTH_BYPASS` when `NODE_ENV=production` | **false** |
| Customer-surface failures | **33** (unchanged vs Phase 1.1 register) |

## 6. Application validation

| Surface | Result |
|---------|--------|
| `/app?...#/opportunity-radar` signed-in | **BLOCKED_AUTH** (Sign in to continue) |
| Embed ranked-list / HE (prior closeout) | PASS historically; not a substitute for signed-in app claim |

Screenshot: `screenshots/phase1-final-recon-app-signin.png` (to be copied beside this report).

## 7. Recommendation

**MERGE READY — CONDITIONAL**

Ready for founder-approved merge of Hotel Explorer code after CI on `a373fa579` is green, with these explicit caveats:

1. Signed-in `/app` Opportunity Radar path not automation-validated (**BLOCKED_AUTH**).
2. Pre-existing ADP customer-surface failures (33) still block **customer-surface production claims**.
3. Permanence Layer B still blocked on durable evidence.
4. Census migration remains a separate follow-on PR.

**Do not deploy. Do not merge without founder approval.**
