# Circuit Breaker Design (V3)

Stop writes immediately if:
- unexpected Airtable schema change (field count / table id mismatch)
- duplicate insert detected on pre-INSERT re-query
- identity conflict on attempted write
- Cvent leakage (`cvent_used_as_production_evidence`)
- legacy evidence leakage
- provenance missing
- write response mismatch vs expected
- unexpected linked-record mutation
- rollback data missing
- error rate > 5%

## Pilot progression
- **Pilot A:** first 25 records
- **Pilot B:** remaining to ~150 only if Pilot A has 0 identity errors, 0 Cvent/legacy leakage, 0 duplicate inserts, 0 unintended overwrites, 0 rollback failures
