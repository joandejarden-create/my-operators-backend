# Oct 29 Last Research — exact source

## Exact producer

| Field | Value |
|-------|-------|
| Manifest | `data/.../hotels/rec0eTEnCqlsGJJt9/weekly-refresh/runs/gdi_weekly_rec0eTEn_2026-10-29T12-00-00-000Z_c2ef69.json` |
| runId | `gdi_weekly_rec0eTEn_2026-10-29T12-00-00-000Z_c2ef69` |
| dryRun | `true` |
| clockLabel | `isolated_future_clock_2026-10-29_not_second_live_week` |
| startedAt | `2026-10-29T12:00:00.000Z` |
| Real wall clock when written | `endedAt` ≈ `2026-10-15T10:30:17Z` (test ran on Oct 15 with +2 week simulated clock) |

Recovery path `recoverHotelActivityLedgerFromManifests` copied the manifest into `activity/ledger-v1.json` with `at: 2026-10-29T12:00:00.000Z` and `status: completed`. `buildHotelActivityHistory` then took `Math.max` across research stamps → customer **Last Research Oct 29, 2026**.

## Classification

| Category | Applies? |
|----------|----------|
| Test-clock simulation | **Yes** (`isolated_future_clock_…`) |
| Dry run / plan-only refresh | **Yes** (`dryRun: true`; queue-building pilot) |
| Fixture / replay | No (local pilot journal, not fixture pack) |
| Imported evidence date | No |
| Actual completed live research | **No** |

## Correction (this release)

- Customer aggregation excludes dry-run, test/simulated clocks, and timestamps beyond now + 2h skew.
- Original timestamps retained on audit ledger events (`customerHistoryEligible: false`).
- Live orchestrator refuses dry-run / clockLabel writes into the production activity ledger.
- `runId` alone no longer invents **Completed**.
- Last Research uses eligible executed research only; source verification is labeled separately when present.
