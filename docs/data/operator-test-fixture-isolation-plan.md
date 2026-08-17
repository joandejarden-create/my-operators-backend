# Operator Test Fixture Isolation Plan

**Date:** 2026-08-09  
**Status:** Recommendation only — **do not delete** Airtable records now

---

## Immediate

1. Treat the following Masters as **Beta / Dummy** (see universe audit):
   - Antillano Norte Hospitality Group (`recTUjuDxL96yWcQA`) — documented sample/demo
   - Cordillera One Gestión, Viento Sur Gestión Hotelera, Mangle Azul Hospitalidad, Panamerican Lodging Partners S.A., Río Plata Hotel Partners, Barrio Hotelero CDMX, Metro Lodging São Paulo, Oro Verde Lodge & Hotel Operators
2. Exclude from:
   - Operator Explorer production list (enable `OPERATOR_EXPLORER_HIDE_TEST_RECORDS` or status filter)
   - Fit production Active universe
   - Research waves
   - Explorer Publishable / Fit Data Ready promotion
3. Prefer additive **Record Purpose = Test Fixture** (or `submission_status` value **Test Only** if founder chooses one taxonomy — not both).
4. Keep records for factory demos until code fixtures fully replace them.

---

## Medium term

- Move testing to **code fixtures** (`fixtures/operator-*.json`) + local gold mock  
- Stop seeding new fake companies into Airtable  
- Research Stage reserved for **real** companies only (Álvarez, Tremun, AADESA pattern)

---

## Long term

- Remove dependency on fake Airtable companies where feasible  
- Archive or Do Not Use after consumers migrated  
- Never silently delete without dependency proof

---

## Real Research Stage (do not treat as dummy)

- Álvarez Argüelles Hoteles  
- Tremun Hoteles  
- AADESA  
