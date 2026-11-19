# GDI 3.0 Slice 2A — Persistence Honesty

**Status:** Implementation PR (founder review before merge)  
**Mode:** $0 code + dry-run tests only  
**Non-goals:** Slice 2B/2C · production Airtable writes · schema mutations · paid research · default-on Airtable reuse

## What changed

1. **`chain.persistenceOpportunities`** — full commercial opportunities carried alongside thin `processed[]` scorecards (trace omits bulk).
2. **`gdi-research-inventory-slim-payload-v1`** — allowlist slim payload preserving identity, evidence URLs, contacts/roles, fit, partners, competitor fields, continuation, run economics, commercial brief inputs. Strips sales/hotel-private/publication/telemetry.
3. **Bridge** — prefers `input.opportunities` → `chain.persistenceOpportunities` → ORS reload (`GDI_DATA_ROOT` aware) → thin row (fail-closed). Lossless guard fails when contacts/evidence disappear or thin scorecards are used as the sole source. Build/upsert failures set `ok:false`.
4. **Store** — default serialization uses slim payload; `sourceSummary` also reads `evidence[]` URLs. 95k fail-closed retained.
5. **Tests** — Bethesda / Radisson / YOTEL frozen fixtures prove retention, thin-row failure, ORS reload, idempotency, sales-key strip, reconstruction.

## Results

See `reports/gdi-slice2a-persistence-honesty-v1/FOUNDER_RESULTS.md`.

## Remaining (not in 2A)

- Typed contact/evidence columns on Research Opportunities (JSON sufficient for now)
- DG / Program / Signal / Fit wiring → **Slice 2B**
- Default-on Airtable reuse → after identity/confidentiality validation
- ORS gitignored local files are not production-durable without mirrored http evidence URLs

## Acceptance

Run: `npm run test:gdi-slice2a-persistence-honesty-v1`
