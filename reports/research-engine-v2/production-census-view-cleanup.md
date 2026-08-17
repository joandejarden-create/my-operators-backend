# Production Census View Cleanup
**Status:** `production_census_views_manual_ui_steps_needed`
**Generated:** 2026-08-05T11:24:51.339Z
**Base:** Deal Capture Platform (`appCCU…foLk`)
**Table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)
## 1. Executive summary
- Views found: 4
- Views missing: 0
- API view visibility/order update supported: false
- View order applied via API: false
- Steward filter applied via API: false
- Census validation pass: true
- Field aliases: Brand → Current Brand; Source Family → Family / Source Family
## 2. Views found
```json
[
  {
    "id": "viwP3l0OlDSuu8Zv9",
    "name": "Census - Core Identity",
    "type": "grid",
    "visible_field_count": 12
  },
  {
    "id": "viwQihvRTmm1wLieT",
    "name": "Census - Enrichment",
    "type": "grid",
    "visible_field_count": 95
  },
  {
    "id": "viwM3RV9jSrM10RGM",
    "name": "Census - Owner Operator",
    "type": "grid",
    "visible_field_count": 95
  },
  {
    "id": "viwYpkZ7zrSt7LBb1",
    "name": "Census - Steward Review",
    "type": "grid",
    "visible_field_count": 95
  }
]
```
## 3–4. Fields shown / hidden per view
### Census - Core Identity
Purpose: Daily review of the core property record.
**Show (13), ordered:**
1. Property Name
2. Current Brand
3. Affiliation Status
4. City
5. State / Region
6. Country
7. Source URL
8. Family / Source Family
9. Data Eligible
10. Data Confidence Tier
11. Production Use Status
12. Enrichment Status
13. Human Review Required
**Hidden:** 82 fields (all others on the table).
**Aliases:** Brand → Current Brand; Source Family → Family / Source Family
**Currently matches target visibility/order:** false
### Census - Enrichment
Purpose: Description, amenities, property type, and asset-context enrichment.
**Show (23), ordered:**
1. Property Name
2. Current Brand
3. City
4. State / Region
5. Country
6. Hotel Description - Source Text
7. Hotel Description - AI Summary
8. Amenities - Source Text
9. Amenities - Structured Tags
10. Property Type
11. Asset Context
12. Market / Submarket
13. F&B Flag
14. Meeting Space Flag
15. Resort / Leisure Flag
16. Extended Stay Flag
17. Mixed-Use Flag
18. Branded Residences Flag
19. Source URL
20. Enrichment Status
21. Enrichment Priority
22. Data Confidence Tier
23. Human Review Required
**Hidden:** 72 fields (all others on the table).
**Explicitly keep hidden (over-modeled):** Fitness Flag, Pool Flag, Parking Flag, Airport Shuttle Flag, Spa Flag, Beach / Waterfront Flag
**Aliases:** Brand → Current Brand
**Currently matches target visibility/order:** false
### Census - Owner Operator
Purpose: Later owner/operator sourcing. Keep clean and mostly empty for now.
**Show (15), ordered:**
1. Property Name
2. Current Brand
3. City
4. State / Region
5. Country
6. Owner Name
7. Owner Confidence
8. Operator / Management Company
9. Operator Confidence
10. Ownership Review Status
11. Operator Review Status
12. Source URL
13. Data Confidence Tier
14. Enrichment Status
15. Human Review Required
**Hidden:** 80 fields (all others on the table).
**Aliases:** Brand → Current Brand
**Currently matches target visibility/order:** false
### Census - Steward Review
Purpose: Human review queue.
**Show (14), ordered:**
1. Property Name
2. Current Brand
3. Affiliation Status
4. City
5. State / Region
6. Country
7. Human Review Required
8. Brand-Unassigned Reason
9. Notes for Steward
10. Enrichment Priority
11. Data Confidence Tier
12. Source URL
13. Production Use Status
14. Enrichment Status
**Hidden:** 81 fields (all others on the table).
**Aliases:** Brand → Current Brand
**Currently matches target visibility/order:** false
## 5. Whether view order was applied
- Applied via API: **false**
- Reason: Airtable Meta API has no supported endpoint to update existing view visibleFieldIds / field order / filters (PATCH/PUT return 404).
## 6. Whether Steward Review filter was applied
- Applied via API: **false**
- Manual: Filter Census - Steward Review where Human Review Required is checked.
- Expected records after filter: **4**
## 7. Manual UI steps needed
### General
- In Airtable: open the view → click Hide fields (eye icon / field visibility).
- Prefer: Hide all → then toggle on only the listed fields in order (drag to reorder after unhiding).
- Do not delete fields from the table. Hiding is view-local only.
- Do not change filters on Core Identity / Enrichment / Owner Operator unless a filter already exists.
- For Steward Review only: Filter where Human Review Required is checked (expect 4 records).
### Census - Core Identity
- Open Deal Capture Platform → Hotel Property Census → view “Census - Core Identity”.
- Open the Hide fields control (or right-click column headers).
- Hide every field except the 13 listed below (or Hide all, then unhide only these).
- Drag columns left-to-right into this exact order:
-   1. Property Name
-   2. Current Brand
-   3. Affiliation Status
-   4. City
-   5. State / Region
-   6. Country
-   7. Source URL
-   8. Family / Source Family
-   9. Data Eligible
-   10. Data Confidence Tier
-   11. Production Use Status
-   12. Enrichment Status
-   13. Human Review Required
### Census - Enrichment
- Open Deal Capture Platform → Hotel Property Census → view “Census - Enrichment”.
- Open the Hide fields control (or right-click column headers).
- Hide every field except the 23 listed below (or Hide all, then unhide only these).
- Drag columns left-to-right into this exact order:
-   1. Property Name
-   2. Current Brand
-   3. City
-   4. State / Region
-   5. Country
-   6. Hotel Description - Source Text
-   7. Hotel Description - AI Summary
-   8. Amenities - Source Text
-   9. Amenities - Structured Tags
-   10. Property Type
-   11. Asset Context
-   12. Market / Submarket
-   13. F&B Flag
-   14. Meeting Space Flag
-   15. Resort / Leisure Flag
-   16. Extended Stay Flag
-   17. Mixed-Use Flag
-   18. Branded Residences Flag
-   19. Source URL
-   20. Enrichment Status
-   21. Enrichment Priority
-   22. Data Confidence Tier
-   23. Human Review Required
- Confirm these over-modeled amenity flags stay hidden: Fitness Flag, Pool Flag, Parking Flag, Airport Shuttle Flag, Spa Flag, Beach / Waterfront Flag.
### Census - Owner Operator
- Open Deal Capture Platform → Hotel Property Census → view “Census - Owner Operator”.
- Open the Hide fields control (or right-click column headers).
- Hide every field except the 15 listed below (or Hide all, then unhide only these).
- Drag columns left-to-right into this exact order:
-   1. Property Name
-   2. Current Brand
-   3. City
-   4. State / Region
-   5. Country
-   6. Owner Name
-   7. Owner Confidence
-   8. Operator / Management Company
-   9. Operator Confidence
-   10. Ownership Review Status
-   11. Operator Review Status
-   12. Source URL
-   13. Data Confidence Tier
-   14. Enrichment Status
-   15. Human Review Required
### Census - Steward Review
- Open Deal Capture Platform → Hotel Property Census → view “Census - Steward Review”.
- Open the Hide fields control (or right-click column headers).
- Hide every field except the 14 listed below (or Hide all, then unhide only these).
- Drag columns left-to-right into this exact order:
-   1. Property Name
-   2. Current Brand
-   3. Affiliation Status
-   4. City
-   5. State / Region
-   6. Country
-   7. Human Review Required
-   8. Brand-Unassigned Reason
-   9. Notes for Steward
-   10. Enrichment Priority
-   11. Data Confidence Tier
-   12. Source URL
-   13. Production Use Status
-   14. Enrichment Status
- Filter: Filter Census - Steward Review where Human Review Required is checked.
- Expected after filter: 4 held records (Human Review Required checked).
## 8. Census validation
```json
{
  "record_count": 666,
  "field_count": 95,
  "duplicates": 0,
  "v111_renames_present": {
    "Last Reviewed Date": true,
    "Resort / Leisure Flag": true,
    "Extended Stay Flag": true
  },
  "old_names_absent": {
    "Last Verified Date": true,
    "Resort Amenities Flag": true,
    "Extended Stay Amenity Flag": true
  },
  "overmodeled_still_exist": true,
  "amenity_filled": {
    "Fitness Flag": 0,
    "Pool Flag": 0,
    "Parking Flag": 0,
    "Airport Shuttle Flag": 0,
    "Spa Flag": 0,
    "Beach / Waterfront Flag": 0
  },
  "enrichment_not_started": 666,
  "human_review_true": 4,
  "production_use_ok": 666,
  "description_filled": 0,
  "amenities_filled": 0,
  "owner_filled": 0,
  "operator_filled": 0,
  "rooms_filled": 0,
  "opening_filled": 0,
  "zero_zero": 0,
  "no_schema_writes": true,
  "no_record_writes": true,
  "pass": true
}
```
## 9. Brand Explorer safety result
```json
{
  "gates": [
    {
      "label": "active_universe_sot",
      "ok": true,
      "exit_code": 0,
      "command": "npm run brand-explorer-active-universe-source-of-truth -- --dry-run"
    },
    {
      "label": "global_active_semantic",
      "ok": true,
      "exit_code": 0,
      "command": "npm run brand-explorer-global-active-semantic-audit -- --dry-run --fresh"
    },
    {
      "label": "pvql_quiet",
      "ok": true,
      "exit_code": 0,
      "command": "node scripts/brand-explorer-quiet-sequential-pvql.mjs"
    },
    {
      "label": "momentum_evidence",
      "ok": true,
      "exit_code": 0,
      "command": "npm run test:brand-explorer-recent-momentum-evidence-quality"
    },
    {
      "label": "mandatory_release_gates",
      "ok": true,
      "exit_code": 0,
      "command": "npm run test:brand-explorer-mandatory-release-gates"
    }
  ],
  "all_pass": true,
  "active_universe": 62,
  "semantic": {
    "critical": 0,
    "high": 0,
    "medium": 0
  },
  "summary": {
    "active_universe": 62,
    "semantic_c_h_m": "0/0/0",
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS"
  },
  "expected": {
    "active_universe": 62,
    "semantic_c_h_m": "0/0/0",
    "pvql": "PASS",
    "momentum": "PASS",
    "mandatory_gates": "PASS"
  }
}
```
## 10. Final recommendation
Complete the manual Hide fields + column order steps for all four views (and Steward Review filter). Then freeze the Census field contract and begin descriptions + amenities enrichment.