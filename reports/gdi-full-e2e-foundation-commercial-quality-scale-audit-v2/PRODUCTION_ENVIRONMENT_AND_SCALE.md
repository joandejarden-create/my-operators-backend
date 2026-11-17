# Production, Environment, and Operational Scale (Audit L)

## Version alignment

| Layer | Evidence |
|-------|----------|
| GitHub `main` | `a4224cc99` at audit start |
| Primary checkout | Same SHA after sync discipline |
| Publication fingerprints | Per-hotel CURRENT (YOTEL `35343009…`) |
| Railway production | Serves deployed SHA + CURRENT packages; not re-verified live in this pass beyond prior YOTEL share QA |
| localhost:8080 | Not required running for this audit |

Code and data versions must be jointly certified (fingerprint + git SHA) before client share — YOTEL path demonstrates this.

## Monitoring

Execution Monitor (PR #336) provides per-hotel JSON ledgers, certificates, provider lists. Historical runs correctly retain UNKNOWN. **Gap:** live Full E2E must always write ledgers (FND-002).

## Scale readiness (realistic)

| Scale step | Likely bottleneck |
|------------|-------------------|
| Tens of hotels | Campaign-script sprawl; manual founder approval OK |
| Hundreds | Provider cost, dedup, shared evidence missing, observability volume |
| Multi-hotel markets | Rediscovery spend without shared evidence (FND-011) |
| Weekly auto | Blocked on Weekly Change + identity (FND-007/010) |

Prefer instrumentation and identity before infra rewrite.
