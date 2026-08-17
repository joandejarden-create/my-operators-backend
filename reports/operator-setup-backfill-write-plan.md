# Operator Setup Backfill Write Plan (DRY-RUN ONLY)

**Not applied.** See `data/operator-setup/audit/operator-setup-backfill-write-plan.json`.

| Metric | Count |
| ------ | ----: |
| Proposed mutations | 59 |
| Derived Active Countries | 14 |
| Derived Brand Families | 0 |
| Derived location/dev flags | 45 |

## Sample

- Playa Hotels & Resorts: `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Hilton (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico","Panama"] (DERIVED; Market Presence Current + Assignments)
- Hilton (Managed): `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Hilton (Managed): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Remington Hospitality: `Operator Setup - Platform & Markets`.`Active Countries` ← ["United States","Dominican Republic"] (DERIVED; Market Presence Current + Assignments)
- Remington Hospitality: `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Remington Hospitality: `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- IHG Hotels & Resorts (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Panama","Mexico","Cayman Islands","Colombia"] (DERIVED; Market Presence Current + Assignments)
- IHG Hotels & Resorts (Managed): `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- IHG Hotels & Resorts (Managed): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Minor Hotels (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico","France"] (DERIVED; Market Presence Current + Assignments)
- Minor Hotels (Managed): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- AADESA: `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- AADESA: `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Accor (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico","Colombia","Barbados"] (DERIVED; Market Presence Current + Assignments)
- Accor (Managed): `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Accor (Managed): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Arbor Lodging (CALA): `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Arbor Lodging (CALA): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Arbor Lodging (CALA): `Operator Setup - Platform & Markets`.`conversionExperience` ← "Yes" (DERIVED; Assignments.Development Context)
- Aimbridge Hospitality (LATAM): `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Aimbridge Hospitality (LATAM): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Marriott International (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico","Panama","United States Virgin Islands"] (DERIVED; Market Presence Current + Assignments)
- Marriott International (Managed): `Operator Setup - Profile & Positioning`.`locationTypeResort` ← "Yes" (DERIVED; Assignments.Urban / Resort)
- Marriott International (Managed): `Operator Setup - Platform & Markets`.`locationTypeUrban` ← "Yes" (DERIVED; Assignments.Urban / Resort)
