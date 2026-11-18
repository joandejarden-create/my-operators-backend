# Weekly automation design

Enabled now: **false**

1. Load existing Airtable research inventory for hotel (+ shared generators by identity)
2. Identify relevant existing organizations / opportunities
3. Run 14-segment discovery under paid ceiling + stage budgets
4. Enrich existing opportunities before rediscovering
5. Discover new demand; triage geography / unsupported / duplicates
6. Selective deep + contact verification
7. Apply commercial qualification (CONFIRMED / RELIABLE_INFERRED only for GDO path)
8. Upsert research inventory under bounded production authorization
9. Update GDO only under distinct GDO policy
10. Calculate Weekly Change via gdi-weekly-change-durability-v1
11. Validate Airtable read-back
12. Emit operator execution report

## Reuse

- `gdi-canonical-full-e2e-entry-v1`
- `gdi-full-e2e-research-persistence-bridge-v1`
- `gdi-hotel-research-inventory-store-v1`
- `gdi-weekly-change-durability-v1 / weekly-delta.js`
- `gdi-segment-stage-call-budget-v1`

Recurring execution: **NOT_ENABLED**
