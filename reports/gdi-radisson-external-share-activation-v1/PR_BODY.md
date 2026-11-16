## Summary
- Founder approved fingerprint `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`.
- Advances Radisson Blu Toronto Downtown to CUSTOMER_PUBLISHED CURRENT (66 = 50 HM + 16 CMI).
- Records durable founder approval ledger + receipt; updates parity gate for live PACKAGE share mode.
- Does **not** commit share token secrets. Token issuance happens after Railway deploy via `railway run`.

## Test plan
- [x] `npm run test:gdi-radisson-external-share-activation-preflight-v1`
- [x] `npm run test:gdi-radisson-external-share-parity-v1`
- [ ] CI green
- [ ] After merge: production CURRENT fingerprint match
- [ ] After deploy: issue hotel-scoped share token + real-browser 66/66 parity
