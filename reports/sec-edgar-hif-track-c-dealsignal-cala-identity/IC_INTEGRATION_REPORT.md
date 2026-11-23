# IC — Track C DealSignal CALA Identity Integration

**As of:** 2026-11-23  
**Source workspace (preserved dirty):** `C:\Dev\dealality\ws-hif-v6-track-c` @ `f25fb7d6b`  
**IC branch:** `feat/hif-track-c-dealsignal-cala-identity-handoff-v1` from `origin/main` (`baa2b8cc8` / #555 tip)  
**Publication:** `CUSTOMER_BLOCKED` · Paid research: **$0** · No Airtable re-apply

---

## 1. Workspace preservation

| Item | Status |
|---|---|
| Track C branch | `feat/hif-v6-track-c-wave3b-opportunity-exa` |
| HEAD | `f25fb7d6b` (unchanged) |
| Untracked Exa cache / ledger / identity artifacts | **Preserved** — no reset/clean/stash/switch |
| Integration primary | Dirty preserved (`SKIP_INTEGRATION_DIRTY`) |

CALA identity outputs were **untracked** on Track C (not yet in #551). IC copied reports + scripts into an isolated worktree — did **not** mutate Track C Git state.

---

## 2. Ledger vs founder prompt reconciliation

Founder prompt stated “8 new HPC + 1 existing.”  
**Authoritative artifacts disagree:**

| Metric (APPLY ledger / identity table) | Value |
|---|---:|
| Examined | 9 |
| CALA examined | 2 |
| HPC `MATCHED_EXISTING` | **9** |
| HPC creates | **0** |
| Inventory linked | **9** |
| Inventory created | **0** |
| Read-back OK | **9** |
| Idempotency | all `idempotencyOk: true` |

**Interpretation:** This wave **linked** discovery inventory to **already-existing** HPC records (no new HPC creates). Do not re-run `--apply-hpc-create`. Treat “8 creates” as **incorrect relative to committed ledgers** — IC uses ledger truth.

---

## 3. Nine canonical identities (verified from table + HPC read-back)

| Hotel | Country / city | HPC | Inventory | Finding ID | Owner | Operator |
|---|---|---|---|---|---|---|
| Hyatt Ziva Riviera Cancún | Mexico / Puerto Morelos | `recrC9yHg42lwo4mr` | `recgOEt0Q2bX81PGS` | `find_tb_v3_hyatt_ziva_rc_not_in_tortuga_13` | null (explicit) | null (explicit) |
| Hyatt Zilara Riviera Maya | Mexico / Playa del Carmen | `rec5epjWiOib5Geab` | `recVLa6xJCptkrxuW` | `find_tb_v3_hyatt_zilara_rm_not_in_tortuga_13` | null | null |
| The Bidwell Marriott Portland | US / Portland | `rectg1ODXj9atq2wP` | `recduaqOt7KTqBuuj` | `find_ds_opew_v2_bidwell_portland_bct_2026` | Sunstone | Sage |
| Residence Inn Long Island Holtsville | US / Holtsville | `recJrDYFvw7fbhv0O` | `recvC7f9cA0LSDloR` | `find_ds_opew_v2_ri_holtsville_bct_2026` | Chatham | IHM |
| Crowne Plaza Atlanta Perimeter at Ravinia | US / Atlanta | `recZVScwZ959oSdTS` | `recLf07AOA1EtCNRC` | `find_ds_opew_v2_crowne_plaza_atlanta_hma_sale_2026` | Service Properties | IHG |
| Margaritaville Hollywood Beach Resort | US / Hollywood | `rec4vYXP0AGWbfqXC` | `rec8G9l1K8lmbvbnE` | `find_ds_eci_v1_margaritaville_hollywood_mct_2026` | Pebblebrook | null |
| Margaritaville Beach House Key West | US / Key West | `rec4YqlSAHD4tAZJr` | `recXOLsjg7Wm3dbsP` | `find_ds_eci_v1_mbhk_mct_2026` | DiamondRock | Ocean Properties |
| Kimpton Hotel Palomar Phoenix | US / Phoenix | `reczqEI4zFgHPfbCt` | `recXv6CryDsRmbVE1` | `find_ds_eci_v1_palomar_phoenix_bct_2026` | DiamondRock | null |
| Westin Denver International Airport | US / Denver | `recBnusACrWtiY0kJ` | `recE2moA2i08tVkNH` | *(null in handoff)* | null | null |

Owner ≠ operator preserved. Ziva/Zilara notes forbid inventing successor manager after Playa termination.

---

## 4. Live DealSignal reader connectivity (`CUSTOMER_BLOCKED`)

Against live `/api/deal-opportunities` (385 opps):

| Hotel | Handoff finding HE | Notes |
|---|---|---|
| Ziva / Zilara | **READY** (exact finding IDs) | MX geo OK; Puerto Morelos vs Cancún label variance on sibling rows |
| Bidwell | **READY** | Exact finding + HPC match |
| Holtsville | **READY** | Exact finding + HPC match |
| Hollywood Beach | HPC ready on **sibling** finding IDs (`…_early_2026` / Track B ISE), not exact handoff ID |
| Beach House KW | HPC ready on sibling `…_bh_kw_mct_early_2026` | Handoff ID `…_mbhk_mct_2026` not present as live id |
| Westin DEN | **READY** on `find_cb5_westin_den_airport_mct_2026` | Handoff omitted findingId |
| Crowne Plaza Ravinia | **PENDING** on live rows seen | Handoff finding id not in live set; sibling `…_cp_atlanta_ravinia_…` still pending |
| Palomar Phoenix | **PENDING** on live early finding | Handoff id not in live set; live `…_kimpton_palomar_phoenix_bct_early_2026` still pending |

Commercial status / sources / CUSTOMER_BLOCKED unchanged. No customer publish.

---

## 5. Git / PR plan

- **Focused PR** from this IC branch: scripts + immutable reports/handoffs only.  
- **Do not** fold into stale #551 tip without main sync (AHIP/P0 already on that branch; identity was untracked).  
- **Do not** cherry-pick older HIF modules over newer main (#555 already merged).  
- **Do not** re-apply Airtable.  
- Exa cache stays untracked on Track C home (not packaged).

---

## 6. Remaining handoffs

1. Align sibling DealSignal finding IDs (Palomar / Margaritaville / Crowne Plaza) so reader HE flags attach to the same finding rows as Track C inventory links — **without inventing ownership**.  
2. Merge Track B #556 after #555 (done); keep #549 from overwriting newer contracts.  
3. Keep #551 for AHIP/P0 code already committed; this IC PR carries the DealSignal CALA identity evidence package.  
4. No full HIF automation engine.

---

## Final IC state

`AWAITING_CI_THEN_MERGE` for focused identity handoff PR · Track C dirty home **PRESERVED** · Airtable writes **NOT repeated** · `CUSTOMER_BLOCKED`
