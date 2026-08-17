# Phase C OE Adapters

**Version:** operator-setup-phase-c-oe-adapters-v1

## oe-brand-adapter

- **Old input:** golden brand JSON / deepen packs
- **New input:** Operator Intelligence - Brand Relationships + Assignments.Brand
- **Output:** Setup Brand Relationships section rows (Brand Snapshot + Portfolio Mix)
- **Null:** no brands → no rows
- **Conflict:** skip if operator already has ≥3 Setup BR rows

## oe-operating-adapter

- **Old input:** Arbor/HE fixtures (`apply-arbor-cala-operating`)
- **New input:** Assignments (Current named) + Market Presence current
- **Output:** thin Capability rows (Platform Snapshot, multi-market, structures, development, hotel-type evidence)
- **Null:** <2 named current assignments → no rows
- **Conflict:** skip if operator already has ≥5 Operating Platform rows
- **Never:** invent KPI levels, portfolio %, Fit prefs

Module: `lib/operator-setup/phase-c-oe-adapters.js`
