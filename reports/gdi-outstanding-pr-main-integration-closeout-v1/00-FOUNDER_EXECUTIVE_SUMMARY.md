# GDI Outstanding Integrations Closeout V1

**Status:** `GDI_OUTSTANDING_INTEGRATIONS_COMPLETE — MAIN_SYNCED — INTEGRATED_ON_8080 — AWAITING_FOUNDER_LIVE_GO` *(after this PR merges + post-merge sync)*  
**Workspace baseline at implementation start:** `dc839fd25` (= `origin/main` post-#678)  
**Audit accepted:** `reports/gdi-outstanding-pr-main-integration-audit-v1/`  
**Paid spend this assignment:** `$0` · Surfe automatic: none · CURRENT: unchanged · UI: unchanged

## What was done

1. **Closed superseded** PR #578 and PR #496 with founder-authorized GitHub comments.
2. **Integrated #580** — Contact Enrichment usage ledger + Findymail channel contract v1.1; Surfe usage ledger preserved separately (no competing ledger collapse).
3. **Integrated #561** — Person identity collision gates into durable contact store.
4. **Remediated #620** — Authorized LDI module + commercial viability precheck wired into Full E2E (writes `02b-…`); paid LDI still fail-closed without `GDI_LDI_LIVE` + founder auth.
5. **Remediated #589** — Exa Agent quality recovery (person-name + buyer classification); titles alone ≠ direct buyer; permanent `$0` CI.
6. **Implemented inventory membership persistence contract** — durable ledger + Airtable plan (schema authorization required before live GDO field writes); reconstruction cert extended.
7. **PR #645 disposition** — HE Contact column GDI parity; shared contact presentation dependency; **do not merge under this GDI GO** (separate HE authorization).
8. **PR #533** — remains **DEFERRED** (FullEnrich).

## :8080 baseline (reconciled)

| Hotel | ID | STAGED | CURRENT |
|---|---|---:|---|
| Wyndham | `recnPB3Q02NO8Ma1o` | 26 | none (409) |
| Bethesda | `recLuxvwwxID7U2B8` | 68 | **67 preserved** |
| Memmo Príncipe Real | `rec1t35e3sPerzxYu` | 11 | none |
| LaLuna | `recaokE8psIbuVeuH` | 3 | none |
| Condesa DF | `rece0NdV9FCnaFlVS` | 4 | none |

**Recommended Design Hotels Live cert property:** Memmo (EU boutique, LDI-applicable, zero contact channels, contrasts Bethesda). Disclose STAGED 11 starting inventory.

## Remaining before Live GO

1. Founder Live authorization tokens/ceilings (two-hotel prep package).
2. Session injection of `EXA_API_KEY` / `FINDYMAIL_API_KEY` (and `GDI_LDI_LIVE` only if LDI spend authorized).
3. **Schema authorization** for proposed GDO membership fields (or accept payload-embed until authorized).
4. Founder ExactWriteSetAuthorization for Airtable membership/contact persistence applies.
5. Inventory membership Airtable persistence remains **PARTIAL** until authorized writes + live read-back.

## Not claimed

- Live operational Full E2E certification
- Paid provider execution
- Live Airtable schema/data writes
- CURRENT publication
- Worktree retirement
