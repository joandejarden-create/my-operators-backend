# Local ↔ External GDI parity (Radisson)

## Verdict

| Surface | Package | Truncations | 66 drawers |
| --- | --- | --- | --- |
| Local authenticated (`localhost:8080`, defaults to STAGED) | `f2bfebc9…` | **0** | **PASS** (Playwright + API detail) |
| Auth ↔ simulated share (same staged package) | `f2bfebc9…` | **0** | Field-by-field commercial parity **PASS** |
| Live external share (Railway) | CURRENT `533f2642…` | **46 API field hits** | 66 drawers open; **content still truncated** |

## What changed for local founder review

- Localhost authenticated GDI now defaults to `preview=staged` so the corrected candidate package loads without a manual query flag.
- Use `?preview=current` to mirror live external CURRENT.
- Production authenticated hosts remain on CURRENT unless `?preview=staged`.

## Shared pipeline / contracts

- Research packaging uses `softClampCustomerProse` (no hard `.slice(0,260)` cuts).
- Publication quality gate blocks `TRUNCATED_ESSENTIAL_COMMERCIAL_PROSE`.
- Certified drawer commercial-content contract is mandatory in Full E2E template stages 7–9.
- Auth↔share commercial drawer field parity expanded (`COMMERCIAL_DRAWER_PARITY_FIELDS`).

## Preserved fingerprints

- CURRENT: `533f2642f8c74af881ff525c2335f9a5471f5ef1d04ac16f7646131176f8706d`
- Prior Tempo staged immutable: `3f342937994ebaa36769e8c90af56e9cb26effeb940fa8a4de2be6cce4f995a8`
- Corrected staged: `f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d`

## Exact founder approval required

```
FOUNDER_APPROVED_FOR_PUBLICATION f2bfebc96d75b87b9df5ae239b9e48502e117a4a6c51b344e65375a8a5a87e9d
```

Until that phrase is issued, live external CURRENT remains truncated. Local staged + simulated share already certify the corrected package end-to-end.
