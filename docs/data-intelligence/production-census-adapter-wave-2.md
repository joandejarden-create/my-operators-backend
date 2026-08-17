# Production Census Adapter Wave 2

**Status:** `production_census_adapter_wave_2_partial_steward_remaining`

Durable note for Accor + Wyndham + Preferred Autopilot discovery wiring and the production-cycle that followed.

## Authority

- Gap pass: `docs/data-intelligence/webhound-active-brand-coverage-gap-pass.md` (recommendation **C**)
- Reports: `reports/research-engine-v2/production-census-adapter-wave-2.{md,json}`
- Production cycle: `reports/research-engine-v2/autopilot/2026-08-06T11-34-37_CALA-production-cycle`

## What shipped

1. **Accor** Autopilot adapter — continent browse + Catalog API  
2. **Wyndham** Autopilot adapter — sitemap + JSON-LD country filter (not path keywords)  
3. **Preferred** directory adapter — official `/directory`  
4. **Read-only parent inference** — `PARENT_BY_SLUG` for Active Setup routing when Parent Company is blank  

## Production results

- Hotel Property Census **757 → 907**
- **150** High-confidence inserts (Accor 49, Preferred 61, Wyndham 22, Marriott 13, Hilton 5)
- **113** safe updates (Property Type / Asset Context / Address / Market)
- **14** steward insert candidates remaining (Marriott missing city)
- Brand Setup / Brand Explorer / VIC / old Census untouched
- Webhound not invoked

## Operating rules retained

- Only write target: Hotel Property Census `tbl9aY5ijiuIzzWam`
- No owner / operator / developer / dates / Recent Momentum / Company Validated / Brand Verified
- No fuzzy auto-insert; duplicate risk → steward
- Preferred collection labels are not Brand Setup brands

## Next

Defer BWH / SLH / Bunkhouse. Resolve Marriott city steward queue; optionally deepen Accor beyond priority CALA five (Brazil bbox).
