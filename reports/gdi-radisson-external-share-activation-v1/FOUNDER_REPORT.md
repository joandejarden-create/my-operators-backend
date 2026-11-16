# Radisson external GDI — production activation diagnosis

## Verdict

**Production external link is not activated.**  
Root cause is **not** a mysterious Railway outage: there is **no CUSTOMER_PUBLISHED CURRENT package** and **no hotel-scoped Radisson share token**. Ordinary share URLs therefore fail closed with **Access link unavailable**.

**Founder approval for fingerprint `533f2642…` was not found.** Per directive, no publish / token issuance was performed.

| Check | Result |
| --- | --- |
| Production host | `https://my-operators-backend-production.up.railway.app` (up; GDI flag OK) |
| Production STAGED pointer | Present (`533f2642…`) |
| Production CURRENT pointer | **404 / absent** |
| Local CURRENT | **null** |
| Share registry Radisson tokens | **none** |
| Founder approval ledger entry for this fingerprint | **ABSENT** |
| Invalid share resolve | `403 SHARE_MALFORMED` → customer copy “Access link unavailable” |
| Staged inventory | 66 = 50 HM + 16 CMI (integrity OK) |
| CUSTOMER_PUBLISHED | **No** |
| Production share URL | **None issued** |

## Root cause of “Access link unavailable”

The share page (`share-app.js`) shows that message for any failed share capability verification (missing, malformed, revoked, bad signature, hotel-scope mismatch).

For Radisson specifically:

1. **No CURRENT / CUSTOMER_PUBLISHED package** — customer share resolver has nothing approved to serve.
2. **No ACTIVE `gdishare.v1` token** for `recH9h3McgMNFQEla` in the share registry.
3. Opening share without a valid token (or with a placeholder/malformed token) returns **SHARE_MALFORMED** / inactive-link UI.

This is expected fail-closed behavior, not a content defect in the 66-opportunity staged package.

## Founder approval status

- Durable ledger path `data/group-demand-intelligence/gdi-founder-approval-v1/approval-ledger-v1.json`: **does not exist**.
- No receipt authorizing fingerprint  
  `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`.
- This diagnostic / activation request is **not** treated as approval.

## Exact outstanding approval operation

Reply with this exact phrase (fingerprint must match):

```text
FOUNDER_APPROVED_FOR_PUBLICATION 533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d
```

Then authorize running:

```bash
node scripts/gdi-activate-radisson-toronto-533f2642-approved.mjs --apply --confirm-fingerprint=533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d
```

After CURRENT is on main + Railway:

```bash
railway run node scripts/gdi-issue-share.mjs --hotelId=recH9h3McgMNFQEla --label="Radisson Blu Toronto Downtown GDI"
```

Keep the raw token out of GitHub. Validate the HTTPS share URL in a real browser (66/66 drawers).

## Production fingerprint

Exact staged (and only) package fingerprint on production + local:

`533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`

Inventory verified: **66** = **50** Hotel Market + **16** CMI. Integrity OK.

## Railway / browser

- Railway is serving the staged package and GDI flag.
- CURRENT remains 404 → no customer publication to resolve.
- Invalid share probe: `403 SHARE_MALFORMED` (fail closed).
- **No production browser parity run** — no valid external link exists yet.

## Secure external link

**Not issued.** Blocked on founder fingerprint approval.

## Remaining blockers

1. Explicit founder phrase: `FOUNDER_APPROVED_FOR_PUBLICATION 533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`
2. Write `FOUNDER_APPROVED_FOR_PUBLICATION.json` from the template (`approved: true`)
3. Run gated activate script with `--apply` + confirm fingerprint
4. PR/merge CURRENT pointer; Railway deploy; issue share via `railway run`
5. Real-browser 66/66 external parity

## What was not done

- No CUSTOMER_PUBLISHED / CURRENT write
- No share token issuance
- No Airtable writes
- No paid research
- No opportunity content changes
