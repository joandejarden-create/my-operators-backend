# Onboarding Reusability — Post-Bethesda Model (YOTEL as first new-hotel test)

## Reusable (standard automation)

1. **HPC gated stewardship create** — dry-run → confirms → `--enable-production-writes` → dedupe → insert only on `AIRTABLE_BASE_ID_ALT` / Hotel Property Census  
2. **Alias map + GDI hotel config + ADP fixture stub** before HI/GDI tooling  
3. **`onboardHotelIntelligence` / completeness backfill** — domains + ADP attribute sync + gate  
4. **`sync:hotel-adp-attributes`** dry-run/apply with audit markdown  
5. **`gdi-hotel-onboard-seed`** Fit + Research Targets → `WEEKLY_READY`  
6. **Demand territory object** with CORE/NEARBY/COMPETITIVE/STRETCH keywords (no hardcoded scoring)  
7. **Need periods NOT_PROVIDED** allowed with HI_COMPLETE  

## Market-specific (not hotel-hardcoded)

- Source families and languages (ES for Galicia; Caribbean housing pages; CH/FR/EN for Lake Geneva)  
- Territory keyword packs  
- Travel-friction rules (Founex ≠ Geneva CBD)  

## Property-specific

- Room count / meeting inventory from first-party  
- Fit bands (64-suite resort vs 237 airport-corridor vs 116 urban AC)  
- Outdoor vs indoor meeting evidence  

## Should become standard automation

1. Stewardship script generator from evidence pack template  
2. Auto-register alias map + GDI config + ADP fixture on HPC create  
3. Post-HI “missingCritical meeting fields” enrichment from official meetings URL  
4. Market-first discovery scaffold parameterized by territory config (not per-hotel copy-paste cycles)  
5. GDI date-field sanitizer before Airtable writes (AC cycle-2 lesson)  

## Anti-patterns avoided

- Hotel-specific architecture forks  
- Fabricated need periods  
- Overwriting certified ADP baselines  
- Bethesda target reuse  
- Surfe / paid enrichment  
- Legacy base writes  
