# Bethesda Exa Authorized Batch — PDA / Chamber / Potomac

**Authorization:** `FOUNDER_APPROVE_BETHESDA_EXA_3CALL_12USD_PDA_CHAMBER_POTOMAC_20261112`  
**Hotel:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Provider:** Exa Agent only · effort `medium` · max 3 calls / $12

## 1. Exact opportunity IDs

| Label | Exact ID | Organization |
|-------|----------|--------------|
| PDA | `disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_` | Parenteral Drug Association (PDA) |
| Chamber | `disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce` | Greater Bethesda Chamber of Commerce |
| Potomac | `gdi_pep_live_c165c897d233` | Potomac Meetings and Events (Potomac Events) |

## 2–3. Spend

| Metric | Value |
|--------|-------|
| Paid Exa calls issued | **3** |
| Actual billed (`costDollars.total`) | **$0.30** |
| Remaining under $12 | **$11.70** |
| Other paid providers | **None** |

### Per-call ledger

| Opportunity | Run ID | Billed |
|-------------|--------|--------|
| PDA | `agent_run_9844e6c6c4d0499291c4ed2276ecc13f` | $0.10 |
| Chamber | `agent_run_fd5a9ed0f3324dd8a8644d06ab843305` | $0.10 |
| Potomac | `agent_run_f85ea28194a5442e91dd7cd8c04234f5` | $0.10 |

## 4–8. Scorecard (before → after)

| Org | Maturity | Contact | Demand confidence | Sales Ready |
|-----|----------|---------|-------------------|-------------|
| PDA | RESEARCH_CONTINUATION → RESEARCH_CONTINUATION | training@pda.org → **Laura Keeley** (TRI Head of Education) | RELIABLE_INFERRED_DEMAND | No |
| Chamber | RESEARCH_CONTINUATION → PARTNERSHIP_WATCH | Clarissa Harris → Clarissa Harris (VP Mktg/Events) | PARTNERSHIP_POTENTIAL_ONLY | No |
| Potomac | PARTNERSHIP_WATCH → PARTNERSHIP_WATCH | Carrie Rice → Carrie Rice (Founder/CMP) | PARTNERSHIP_POTENTIAL_ONLY | No |

### Evidence highlights

- **PDA:** TRI Bethesda Dec 2026 courses confirmed; PDA controls lodging; listed hotels Hilton Garden Inn / Hyatt Regency Bethesda / Residence Inn Bethesda Downtown — **not** Bethesda Marriott; logistics route `vanzant@pda.org` + `training@pda.org`.
- **Chamber:** 75+ events/year; Off the Clock monthly 65+; Clarissa Harris verified; Marriott is an uncommitted venue option only.
- **Potomac:** Carrie Rice founder/CMP; hotel coordination capabilities evidenced; **no** Marriott partnership or bookable demand found.

## 9–10. Persistence / protection

- Research-stage merge persisted (29 bag intact; three Exa-updated IDs).
- GitHub/main `opportunities.json` was **not written by Exa** (writes blocked).
- Current committed store: **54** rows · SHA `54378697b69755a1010f36d19277f4d4cefc85eae3533f5cdc089f9c64f5c6be` (matches `origin/main`).
- Prior local-only dirty observation of 59 rows / SHA `fd438549…` is **not** in git; five `gdi_sc_*` bodies are absent from the committed store. Exa batch did not delete them.
- Read-time union now: **54 + 29 = 83** against committed store.
- No Sales Ready promotions; no customer publication changes.

## 12. Remaining research

1. PDA: confirm Dec 2026 block owner + overflow via logistics/TRI (first outreach; no further Exa required).
2. Chamber: Clarissa outreach for 2027 Off the Clock / installation venue flexibility.
3. Potomac: obtain verified business email; partnership development only.
