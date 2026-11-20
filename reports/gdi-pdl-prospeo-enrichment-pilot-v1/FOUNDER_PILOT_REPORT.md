# GDI PDL + Prospeo Controlled Enrichment Pilot — Founder Report

Generated: 2026-11-20T10:10:25.177Z

## Authorization respected

- PDL ≤ 10 successful credits (consumed: **2**; remaining header ~**697**)
- Prospeo ≤ 10 email credits (consumed: **3**; remaining **97** / was 100)
- Mobile / FullEnrich / Surfe enrich / Apollo: **not used**

## Corrected metrics (post-hoc quarantine)

| Measure | Raw | Corrected |
|---|---:|---:|
| PARTIAL → READY | 0 | **0** |
| PDL HTTP 200 matches | 2 | 2 |
| PDL identity accepted (strict employer) | 2 | **0** |
| PDL LinkedIn verified usable | 0 | **0** |
| Prospeo named emails accepted | 2 | **0** |
| Quarantined | — | **10** |

### Critical findings
1. **Prospeo parser bug:** email sometimes returned as object → serialized as `[object Object]`. Two false accepts (Daryl Simon, Andrew Bartucci) quarantined. Parser fixed in `lib/prospeo/client.js` for future runs — **no re-bill**.
2. **PDL wrong-employer matches:** Abbas → Az Digital Health Consulting (not U of Toronto IHPME); Agathe → Idyom Translations (not OENOVITI). Quarantined under strict employer gate.
3. **Abbas Prospeo:** 1 credit spent, result quarantined (employer/domain mismatch).
4. **Surfe readiness:** no PARTIAL→READY movement; do not claim higher Surfe match rate.

## Credits consumed
- PDL: **2** successful enrich credits (headers show ~697–698 remaining)
- Prospeo: **3** credits (100 → 97)

## Recommendations
- **PDL:** WEAK_THIS_COHORT_FOR_IDENTITY — 2 raw matches both failed strict employer corroboration; keep as optional probe only
- **Prospeo:** ADAPTER_FIXED_PARSER — this cohort yielded 0 safely parsed accepted emails after quarantine; 3 credits spent (2 false-positive object emails + 1 employer-mismatch). Re-test parser on 1–2 contacts before broader use.
- **FullEnrich:** HOLD — not tested; revisit only if public+PDL+Prospeo miss rate stays high
- **Surfe Search:** HOLD — zero search credits; org/role discovery not exact-person
- **Customer-facing:** Public-verified only by default. No provider-derived channels from this pilot enter customer-visible records. PDL embedded permission still needs counsel check before hotel disclosure. Prospeo standard ToS blocks customer disclosure without Order.

## Persistence
INTERNAL_EVALUATION_REPORT_ONLY — no canonical public-source writes; no STAGED→CURRENT.

## STOP
STOP after ten-contact evaluation. No full-database backfill. No mobile. No Surfe/FullEnrich/Apollo.
