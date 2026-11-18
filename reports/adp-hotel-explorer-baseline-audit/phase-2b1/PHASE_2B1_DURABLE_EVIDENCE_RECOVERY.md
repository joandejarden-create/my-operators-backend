# Phase 2B.1 — Durable evidence and certification recovery

**Workspace:** `C:\Dev\dealality\ws-adp`  
**Started from:** `c448e4529` (`ws/adp`, even with `origin/main` at start)  
**Date:** 2026-11-18  
**Scope:** read-only recovery plus narrow contract fixes. No Airtable writes, paid research, customer publication, or deploy.

## 1. Durable evidence recovery

Authoritative source used: `C:\Dev\dealality\authorized-backups\primary-cutover-backup-20261111\data\adp-durable-published` (Railway volume extract; pull logs name the `adp-durable-published` volume). Cross-checked against the primary checkout and `my-operators-backend-adp-cert-main-rc`.

| Check | Result |
|-------|--------|
| Current properties | 38 / 38 in all three trees; property sets identical |
| Current manifest SHA-256 | 0 mismatches across the three trees |
| Report + evidence SHA-256 vs manifest | 76 / 76 match on the authorized backup |
| Hilton current manifest | `95469e5c7634b013a035860890cbdeaddbb14ad3fc6271e552c33919eba940d1` |
| Current evidence bodies | 38 / 38 are empty-item indexes (`items: []`), checksum-valid |
| Historical snapshot evidence | 71 files: 21 full, 50 stubs. 21 properties have at least one full historical evidence file. Bethesda has none |
| Measured-dates export | Present in the backup. SHA-256 `86b6a62229bb447aaec402f02aae9fb580f4c89ec09fc695ec0189d39a5575f0`. Counts: 38 rows, October 2026 = 16, November 2026 = 22 |
| Local materialization | Copied into gitignored `data/adp-durable-published/` without overwriting existing files. The local Radisson Blu Toronto snapshot was preserved (its hash differs from the backup) |
| Committed payloads | None. Measured-dates JSON and `RECONCILIATION_SUMMARY.json` stay untracked local exports |

Layer B presence is recovered. Layer B **content** is not a full evidence corpus: the current evidence pointer for every hotel is an empty index that matches its published checksum. Those stubs were not replaced or invented.

18 filesystem-published hotels still have no runtime observation file in this workspace or in that backup (including `adp_ac_hotel_a_coruna` and `adp_hilton_times_square`). Territory drawers for those periods return `no_data`. Truncated `responseExcerpt` packs were not promoted to exact-response evidence.

## 2. Customer-surface register

Phase 2A baseline, volume absent: **33** failures.

This run, durable registry mounted and synthetic fixtures removed from commercial gates: **50** failures. The increase is the durable-registry hotels now included in the commercial audit, not a loosened gate.

| Gate | Phase 2A | Phase 2B.1 | Disposition |
|------|--------:|----------:|-------------|
| `TERRITORY_SUBJECT_RATES` | 5 | 5 | Still Bethesda. Locked vector is not the current published period. Constants not flipped |
| `BPP` | 24 | 40 | Smoke synthetic removed (2). Nine durable-registry hotels added (~18). Real hotels remain `MEASUREMENT_NOT_ATTEMPTED` / not frozen |
| `BPP_RAW_RESPONSE_PRESENT` | 0 | 1 | `adp_grand_fiesta_americana_chapultepec` |
| Evidence nonempty / readable / client-ready | 3 | 3 | Portfolio still fail-closed. Empty-link audit `totalFail` 427 → 1335 after the registry mount |
| `DISPLACEMENT_COUNT_EVIDENCE_EXACT_PARITY` | 1 | 1 | No longer aborts on `adp_smoke_test_synthetic`. Commercial result: 145 pass / 17 fail rows across 38 hotels. Gate remains FAIL |

Displacement failures that remain are missing support sets on empty durable evidence indexes (Charlotte, Falls Church, Nobu, Arlington, Sheraton Guadalajara, Tampa Airport, UMusic, voco Cancún, Alltra, Fiesta Americana Condesa, Grand Fiesta Americana Chapultepec), Toronto rows with no published support set, and one real reference mismatch: Now Now NoHo vs Four Seasons Hotel New York Downtown (displayed 12, published support 12, runtime rebuild 11).

## 3. Bethesda Marriott readiness

| Stage | Status |
|-------|--------|
| Identity | `adp_bethesda_marriott` / HPC `recLuxvwwxID7U2B8` on filesystem and durable reports |
| Filesystem published current | `adp_period_adp_bethesda_marriott_20261002142154_62428a` (2026-10-02), Live. Business subject rate **50** |
| Locked certification vector | Lives on earlier filesystem report `...20260909091016_9f3a60`. Business **46.4**, leisure **43.2**, couples **36.1**, group **45.5**, family **15**, celebration **46.4** |
| Durable current | `...20261105213117_f695d6` (published 2026-11-06), Live, consideration 24. Business **30**, leisure **22.2**, couples **28.6**, group **17.9**, family **15**, celebration **40** |
| AI observations | October filesystem package has a large evidence file. November durable evidence is an empty-item index. September and October runtime files exist locally; the November runtime file is not in this workspace |
| Competitive positioning / reality gap / actions | Present on both the October filesystem report and the November durable report |
| Evidence-link audit | PASS on the filesystem package (68 links) |
| Persistence | Layer A PASS. Layer B measured-dates PASS locally when the untracked export is present |
| Certification | `certifiedCount` 0. Private matrix absent. Durable manifest has no certification status |
| Publication authorization | Both pointers say Live. That is not certification and was not treated as permission to publish |
| Repeatable certified execution | **Not ready.** Three different rate vectors exist. The gate still compares the customer read (October filesystem package, business 50) with the September lock |

No paid rerun was performed.

## 4. Code and contract fixes

- Commercial displacement parity skips synthetic test properties and records missing support sets instead of aborting on the first hotel. Real hotels still fail the gate.
- Commercial BPP readiness skips the same synthetic ids. Real `MEASUREMENT_NOT_ATTEMPTED` hotels still fail.
- `readPublishedEvidence` loads the checksummed durable snapshot when no filesystem evidence file exists, and refuses a checksum mismatch.

No certification threshold, territory constant, or evidence body was edited.

## 5. Tests

| Gate | Result |
|------|--------|
| `test:adp-permanent-full-e2e-certification-v1` with `ADP_PERMANENCE_REQUIRE_LAYER_B=1` | Layer A PASS (`notProductionProof`). Layer B **AVAILABLE** (Hilton manifest present). Exit 0 |
| `test:adp-e2e-persistence-contract-v1` | Layer A PASS. Layer B **PASS** (Oct 16, Nov 22, period-metrics hotelsMatch 38, live overlay MATCH 2). Exit 0. Depends on the untracked local exports |
| `test:adp-displacement-evidence-count-parity` | FAIL. 17 commercial rows. Synthetic skipped |
| `test:adp-customer-surface-contract` | FAIL. `failureCount` 50. Exit 1 |
| `test:adp-canonical-31-scope-v1` | PASS. 31 hotels, Bethesda true |
| `test:adp-canonical-31-certification-posture-v1` | PASS structure. `certifiedCount` 0. Matrix absent |

Layer B PASS is local reconciliation against the recovered export. It is not a customer-certification claim.

## 6. External dependencies still required

- Full runtime observation files for the 18 published periods absent from the authorized backup.
- Founder decision: pin Bethesda certification to `...20260909091016_9f3a60`, or authorize a re-cert. Do not retarget the lock to October or November rates.
- Governed BPP measurement and peer freeze (paid; not authorized here).
- Private certification matrix, or an explicit decision that `certifiedCount` stays 0.
- Railway volume remains the production mount. This recovery is a local read-only copy, not a production write.

## 7. Next executable milestone

Pin or re-cert Bethesda only, on the September lock versus a newly authorized period. Do not start a 31-hotel paid rerun and do not publish. After that decision, the next coding step is baking non-empty displacement support into durable current evidence for the hotels whose current index is an empty stub — from original responses, not from excerpts.
