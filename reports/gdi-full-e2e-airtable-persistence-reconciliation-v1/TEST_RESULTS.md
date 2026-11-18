# Test results

pm run test:gdi-full-e2e-airtable-persistence-reconciliation-v1 → **10/10 pass**

- Bethesda dry-run planned writes (processed 20): 7
- byTable: {"Demand Generators":3,"Group Demand Opportunities":3,"Private Event Venues":1}
- deferredAuditOnly: 16
- Campaign airtableWrites: 0
- Production writes: blocked
