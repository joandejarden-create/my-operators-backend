# Operator Fit — Pilot Feature-Flag Policy

**Status:** Recommendation · **Pilot not enabled**  
**Date:** 2026-08-04

---

## Global default

`OPERATOR_FIT_ENGINE_V2=0` — globally **off**.

## Server-controlled pilot model (required)

Enablement must be evaluated **server-side** only. Client flags are insufficient.

### Recommended controls

| Control | Purpose |
| ------- | ------- |
| `OPERATOR_FIT_ENGINE_V2` | Global kill switch (default off) |
| `OPERATOR_FIT_PILOT_INTERNAL_PREVIEW` | Internal/admin preview routes |
| `OPERATOR_FIT_PILOT_ACCOUNT_ALLOWLIST` | Member/account IDs |
| `OPERATOR_FIT_PILOT_DEAL_ALLOWLIST` | Deal record IDs |
| Environment guard | `production` requires both allowlists + global=1 |
| Admin override | Explicit support-admin header/session check |

### Evaluation order (deterministic)

1. If global ≠ enabled → legacy OAS only  
2. If environment forbids → off  
3. If request is internal support route + admin → preview OK  
4. Else require account allowlist **and** deal allowlist  
5. Tag responses with `engineVersion: "operator-fit-v2"`  

### Rules

- No client-side-only enablement  
- No accidental all-owner exposure  
- Legacy OAS remains accessible during pilot  
- New and legacy outputs must not overwrite one another  
- Pilot can be disabled immediately via global flag  
- Existing deals unaffected unless allowlisted  

### Explicit this phase

**Do not enable** any owner. **Do not** set global flag on.
