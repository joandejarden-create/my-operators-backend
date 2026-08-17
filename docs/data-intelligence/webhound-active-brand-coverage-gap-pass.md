# Webhound Active Brand Setup Coverage Gap Pass

Learning-only pass for remaining Active/Live Brand Setup parents after Marriott / IHG / Hilton / Choice Autopilot adapters.

**Full report:** `reports/research-engine-v2/webhound-active-brand-coverage-gap-pass.json`  
**Gap list:** `reports/research-engine-v2/active-brand-discovery-coverage-gap-list.md`  
**Webhound:** https://webhound.ai/session/0abff16f-9e6e-4203-ae91-9ec79556cea6

## Status

`webhound_active_brand_coverage_gap_pass_complete_ready_for_adapter_queue`

## Recommendation

**C — Add several low-effort adapters together:** Accor + Wyndham Autopilot wiring first (existing extractors), then Preferred directory adapter. Defer BWH / SLH / Bunkhouse.

## Production constraints

- Hotel Property Census only (`tbl9aY5ijiuIzzWam`) when applying later  
- Brand Setup / Brand Explorer read-only  
- VIC evidence-only  
- Webhound never Census SoT  

## Queue (short)

1. Accor Autopilot discovery  
2. Wyndham Autopilot discovery (country metadata CALA filter)  
3. Preferred `/directory` adapter  
4. BWH steward/seed only  
5. SLH / Bunkhouse steward  
