# Test results — $0 commercial rescore

Generated: 2026-11-18T14:59:23.806Z

| Script | Status |
| --- | --- |
| `inline:hydrated_processed_export_named_contacts_ge_6` | **PASS** |
| `inline:fully_sales_ready_eligibility_ge_1` | **PASS** |
| `inline:whiting_turner_reliable_inferred_after_rescore` | **PASS** |
| `inline:six_opps_customer_publication_ready_false` | **PASS** |
| `scripts/test-gdi-evidence-demand-mapping-v1.mjs` | **FAIL** |
| `scripts/test-gdi-powershell-to-canonical-full-e2e-reconciliation-v1.mjs` | **PASS** |

## FAIL scripts/test-gdi-evidence-demand-mapping-v1.mjs

```
node:internal/modules/run_main:123
    triggerUncaughtException(
    ^

AssertionError [ERR_ASSERTION]: Expected values to be strictly equal:

true !== false

    at file:///C:/Dev/dealality/my-operators-backend/scripts/test-gdi-evidence-demand-mapping-v1.mjs:173:10
    at ModuleJob.run (node:internal/modules/esm/module_job:343:25)
    at async onImport.tracePromise.__proto__ (node:internal/modules/esm/loader:681:26)
    at async asyncRunEntryPointWithESMLoader (node:internal/modules/run_main:117:5) {
  generatedMessage: true,
  code: 'ERR_ASSERTION',
  actual: true,
  expected: false,
  operator: 'strictEqual',
  diff: 'simple'
}

Node.js v22.23.2

```


## Safety assertions (this package)

- No Exa network calls invoked by rescore script
- No Airtable CREATE/UPDATE/DELETE
- Named contacts present on hydrated processed export: **10**
- Fully sales-ready (eligibility) after rescore: **2**
- Customer published: **0**

Run `node scripts/test-gdi-post-exa-zero-cost-commercial-rescore-v1.mjs` for the package harness.
