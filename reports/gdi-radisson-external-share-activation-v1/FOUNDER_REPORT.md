# Radisson external GDI — production activation complete

## Verdict

Founder-approved fingerprint `533f2642…` is **CUSTOMER_PUBLISHED** on production. Hotel-scoped share token issued. Real-browser external parity **PASS** (66 = 50 HM + 16 CMI).

| Check | Result |
| --- | --- |
| Founder approval | **APPROVED** (exact phrase) |
| Production CURRENT | `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d` |
| Inventory | 66 = 50 HM + 16 CMI |
| Railway deploy | SUCCESS (`d88ea59f…`) via GDI production release |
| Share tokenId | `gdisht_61dea330f3610bad6724f362` |
| Browser parity | PASS |
| Invalid token | Fail-closed → Access link unavailable |
| Token secret in GitHub | **No** |

## Root cause (original error)

No CUSTOMER_PUBLISHED CURRENT and no Radisson share token → fail-closed “Access link unavailable”.

## GitHub / Railway

- Publication PR: https://github.com/joandejarden-create/my-operators-backend/pull/315 (`28b34c53b`)
- Release run: https://github.com/joandejarden-create/my-operators-backend/actions/runs/43963057395

## External link

Issued via `railway run` with production signing secret. Delivered to founder out-of-band / chat; raw `gdishare.v1` token is **not** committed.
