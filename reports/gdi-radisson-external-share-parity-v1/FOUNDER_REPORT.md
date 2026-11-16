# Radisson Blu Toronto — External GDI share parity preflight

## Verdict

**Simulated local↔external package parity: PASS (66/66).**  
**Production external URL: NOT ACTIVE — awaiting founder publication approval.**

| Item | Value |
| --- | --- |
| Hotel | Radisson Blu Toronto Downtown (`recH9h3McgMNFQEla`, CNC41) |
| Local staged fingerprint | `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d` |
| External live fingerprint | _(none — NO_CURRENT_PUBLICATION)_ |
| Fingerprints match | **No live share yet** (simulated share matches staged) |
| Inventory | **66 = 50 HM + 16 CMI** |
| Scaling Companies | **0** |
| Local Catering | **6** |
| Contact Buyer (Sales Ready proxy) | **3** |
| Drawer detail (staged) | **66/66 PASS** incl. FHLIP `rx_d_2400ebe4c2bb` |
| Bethesda reference host | `https://my-operators-backend-production.up.railway.app` |
| Airtable | Local live store ~66 vs staged 66 — package remains SoT; no writes performed |
| Founder approval | **REQUIRED** before CUSTOMER_PUBLISHED / share token |
| Production publication | **NOT ACTIVE** |

## Bethesda external architecture (reused)

- Page: `/group-demand-intelligence-share.html?share=gdishare.v1…`
- Host: `https://my-operators-backend-production.up.railway.app`
- Resolver: `resolveShareOpportunitiesFromApprovedPackage` → **CURRENT / CUSTOMER_PUBLISHED only**
- Issue: `railway run node scripts/gdi-issue-share.mjs --hotelId=…`
- Revoke: `node scripts/gdi-revoke-share.mjs --token-id=…`

## What was completed

1. Exact staged fingerprint verified (`533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`).
2. Package integrity verified.
3. Auth staged vs simulated share DTO parity for all 66 IDs.
4. All 66 staged detail resolutions pass (same package).
5. Permanent regression gate added: `npm run test:gdi-radisson-external-share-parity-v1`.
6. Security / rollback / Airtable reconciliation docs written.
7. **Did not** activate CUSTOMER_PUBLISHED, issue a public token, or write Airtable.

## Exact outstanding founder / deploy actions

1. Approve fingerprint `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d` for CUSTOMER_PUBLISHED.
2. Advance CURRENT pointer for publication version `gdi_pub_recH9h3McgMNFQEla_20261116_segment_classification_remediation_v1`.
3. Merge + Railway deploy so production serves that CURRENT package.
4. Issue hotel-scoped production share token (keep raw token out of GitHub).
5. Open the HTTPS share URL and confirm 66/66 drawers + filters (then re-run production browser verification).

Until step 1–4 complete, any claim of a working **production** external Radisson link is false.
