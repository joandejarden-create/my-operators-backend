# Address pipeline

## Root cause
V2.3 `toDiscoveryRecord` did not persist address; classifier never proposed Address.

## Fix
- `buildAddressStaging` + `normalizeAddress` (LATAM-aware)
- Claim-level CORROBORATED_WRITE when official claim exists
- SerpApi-only remains BLOCKED_RIGHTS without suppressing official

## Coverage on V3 freeze cohort
Address staging nonblank: **0/150** (upstream research still required)
