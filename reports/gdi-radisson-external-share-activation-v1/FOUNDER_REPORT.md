# Radisson external GDI — production activation

## Verdict

Founder approved fingerprint `533f2642…`. Local CURRENT is **CUSTOMER_PUBLISHED**. External share token issuance and production browser proof follow merge + Railway deploy.

| Check | Result |
| --- | --- |
| Founder approval phrase | **RECEIVED** |
| Durable ledger | Present |
| Local CURRENT fingerprint | `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d` |
| Inventory | 66 = 50 HM + 16 CMI (66 unique IDs) |
| Share resolver (local) | PACKAGE mode / 66 opportunities |
| Production CURRENT | Pending Railway deploy of this PR |
| Share token | Pending `railway run` after deploy |
| Browser parity | Pending valid HTTPS share URL |

## Root cause of original “Access link unavailable”

No CUSTOMER_PUBLISHED CURRENT and no Radisson share token. Fail-closed share UI — not token expiry alone.

## Exact approval

```text
FOUNDER_APPROVED_FOR_PUBLICATION 533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d
```

## Activation steps completed locally

1. Wrote `FOUNDER_APPROVED_FOR_PUBLICATION.json` + durable ledger entry
2. Ran gated activate script with `--apply` + fingerprint confirm
3. Wrote bundled `CURRENT.json` + package lifecycle `CUSTOMER_PUBLISHED`

## Remaining (post-merge)

1. Railway deploy of merged main
2. Confirm production CURRENT fingerprint match
3. `railway run node scripts/gdi-issue-share.mjs --hotelId=recH9h3McgMNFQEla --label="Radisson Blu Toronto Downtown GDI"`
4. Real-browser 66/66 external parity (token kept out of GitHub)
