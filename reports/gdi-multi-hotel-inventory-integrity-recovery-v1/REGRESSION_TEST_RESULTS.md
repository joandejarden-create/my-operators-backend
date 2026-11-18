# Regression test results

```bash
npm run test:gdi-multi-hotel-inventory-integrity-recovery-v1
```

**10/10 passed**

Also verified:

```bash
npm run test:gdi-founder-local-research-preview-v1
```

## Acceptance mapping

1. Guadalajara Research Preview → store **43** (not 6) — fixed via shared filter bypass  
2. San Diego research **82** preserved  
3. Castillo research **20** preserved  
4. Bethesda published **67** unchanged  
5. YOTEL published **42** unchanged  
6. Radisson published **66** unchanged  
7. No cross-hotel ID leakage in priority stores  
8. STAGED remains stage-specific  
9. Published remains CURRENT-only  
10. Research Preview does not require publication  
11. Suppressed IDs accounted for (not duplicated inventively)  
12–14. Mode/source contract documented; empty research remains recoverable  
15–17. No Airtable / paid / publication changes in this recovery  
