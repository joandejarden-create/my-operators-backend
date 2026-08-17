# Phase A+B Write Plan

**Mode:** apply

| Metric | Count |
| ------ | ----: |
| DIRECT writes | 8 |
| DERIVED writes | 14 |
| Total proposed | 22 |
| Original audit plan mutations | 59 |
| Original Active Countries retained | 14 |
| Original unsafe location/dev Yes→number dropped | 45 |
| Derived conflicts held | 3 |

## DIRECT eval

- **WRITE:** 8
- **ALREADY CORRECT:** 105
- **CONFLICT:** 11
- **GENUINELY UNKNOWN:** 101
- **NOT APPLICABLE:** 9

## Note on original 59-plan

Many original mutations proposed `Yes` for `locationType*` / `conversionExperience`, but those Airtable fields are **numbers (portfolio %)** — writing `Yes` would be invalid. Urban/Resort on Assignments is also empty. Those mutations are **dropped**. Phase B applies **Active Countries** only (option-filtered).

## Sample mutations

- DIRECT Remington Hospitality: `Operator Setup - Master`.`Operator Website` ← "https://www.remingtonhospitality.com/" (profile-deep-pack)
- DIRECT Tafer Hotels & Resorts: `Operator Setup - Master`.`Operator Website` ← "https://www.taferresorts.com/" (profile-deep-pack)
- DIRECT Grupo Presidente: `Operator Setup - Master`.`Operator Website` ← "https://grupopresidente.com.mx/" (profile-deep-pack)
- DIRECT Royalton Hotels & Resorts: `Operator Setup - Master`.`Operator Website` ← "https://www.royaltonresorts.com/" (profile-deep-pack)
- DIRECT Brittain Resorts & Hotels (BRH): `Operator Setup - Master`.`Operator Website` ← "https://brittainresorts.com/" (profile-deep-pack)
- DIRECT Arriva Hospitality Group (AHG): `Operator Setup - Master`.`Operator Website` ← "https://www.arrivahotels.mx/" (profile-deep-pack)
- DIRECT OxoHotel: `Operator Setup - Master`.`Operator Website` ← "https://www.oxohotel.com/en/" (profile-deep-pack)
- DIRECT Grupo Marta Hospitality: `Operator Setup - Master`.`Operator Website` ← "https://www.grupomarta.com/" (profile-deep-pack)
- DERIVED Hilton (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico","Panama"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Remington Hospitality: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Dominican Republic","United States"] (Market Presence (current) + Assignments (Current, named))
- DERIVED IHG Hotels & Resorts (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Colombia","Mexico","Panama"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Minor Hotels (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Accor (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Colombia","Mexico"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Marriott International (Managed): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico","Panama"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Tafer Hotels & Resorts: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Grupo Presidente: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Royalton Hotels & Resorts: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Dominican Republic","Mexico"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Brittain Resorts & Hotels (BRH): `Operator Setup - Platform & Markets`.`Active Countries` ← ["United States"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Arriva Hospitality Group (AHG): `Operator Setup - Platform & Markets`.`Active Countries` ← ["Mexico"] (Market Presence (current) + Assignments (Current, named))
- DERIVED OxoHotel: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Colombia"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Grupo Marta Hospitality: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Costa Rica"] (Market Presence (current) + Assignments (Current, named))
- DERIVED Grupo Iberostar: `Operator Setup - Platform & Markets`.`Active Countries` ← ["Dominican Republic","Mexico"] (Market Presence (current) + Assignments (Current, named))
