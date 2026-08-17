# Production Census v1.1 — Airtable UI Visibility Check

**Status:** `production_census_v11_founder_looking_at_wrong_base_or_table`
**Mode:** read-only
**Generated:** 2026-08-05T10:05:20.270Z

## Verdict

v1.1 fields **exist** on Deal Capture Platform → Hotel Property Census and are **visible in Grid view**. Founder is likely looking at the **wrong base/table**, or expecting **renames / filled enrichment** that were never applied.

## 1. Correct base and tables

- **Correct base:** Deal Capture Platform (`appCCU…foLk` / appCCU…foLk)
- **Correct table:** Hotel Property Census (`tbl9aY5ijiuIzzWam`)

### Do not look here

- Deal Capture MVP (`appvtn…INP6`) — Brand Explorer / Brand Setup
- Deal Capture MVP — Sandbox (`appRbW…2ch1`)
- Brand Setup - Brand Basics
- Brand Setup - Brand Explorer Presentation
- legacy Hotel Census
- Verified Independent Hotel Census stub

### Table presence on Platform

- Hotel Property Census: EXISTS (`tbl9aY5ijiuIzzWam`) · on MVP: false · on Sandbox: false
- Hotel Property Brand Affiliations: EXISTS (`tbll7n0xgmYywyrTd`) · on MVP: false · on Sandbox: false
- Hotel Property Source Evidence: EXISTS (`tblfhosu44nMbvSbS`) · on MVP: false · on Sandbox: false
- Hotel Property Steward Review: EXISTS (`tbluxLjGTuKGRO2iM`) · on MVP: false · on Sandbox: false

## 2. Hotel Property Census state

| Metric | Expected | Actual |
| --- | --- | --- |
| Records | 666 | 666 |
| Fields | 95 | 95 |
| v1.1 fields | 62 | 62 |
| Enrichment Status = Not Started | 666 | 666 |
| Human Review Required = true | 4 | 4 |
| Descriptions filled | 0 | 0 |
| Amenities filled | 0 | 0 |
| Owner filled | 0 | 0 |
| Operator filled | 0 | 0 |
| Rooms filled | 0 | 0 |
| Opening dates filled | 0 | 0 |

### Blank by design

- Hotel Description - Source Text
- Hotel Description - AI Summary
- Short Property Summary
- Amenities - Source Text
- Amenities - Structured Tags
- Owner Name
- Operator / Management Company
- Rooms / Keys
- Opening Date
- Developer Name

### Safe backfill present

- Enrichment Status (= Not Started)
- Human Review Required (= true on 4 held only)
- Data Confidence Tier
- Enrichment Priority
- Independent Classification / soft-brand flags (derived)

## 3. Name changes were NOT applied

- Post-apply review was read-only.
- It recommended possible v1.1.1 cleanup later — it did not rename or delete fields.
- Last Verified Date vs Last Reviewed Date remains a founder decision (not applied).
- Amenity consolidation (move Fitness/Pool/etc. into Structured Tags) was recommended but not applied.
- No fields were renamed or deleted after v1.1 schema create.

## 4. View visibility

- **Grid view** (`viw9zpA4M7PYntHKJ`, grid) — visible fields: 95; v1.1 hidden: 0

API reports all 95 fields (including all 62 v1.1 fields) visible in Grid view. If founder still cannot see them, they are almost certainly in the wrong base/table, or scanning for renamed labels / filled enrichment cells that do not exist yet.

### Field search targets

| Field | Exists | Visible in Grid |
| --- | --- | --- |
| Hotel Description - Source Text | true | true |
| Amenities - Structured Tags | true | true |
| Owner Name | true | true |
| Operator / Management Company | true | true |
| Enrichment Status | true | true |
| Human Review Required | true | true |
| Production Use Status | true | true |

## 5. Airtable UI steps for founder

1. Open Airtable base: Deal Capture Platform (appCCU…foLk) — not MVP, not Sandbox
2. Open table: Hotel Property Census (tbl9aY5ijiuIzzWam)
3. Open view: Grid view
4. Click the field visibility / hidden fields control (or use field search)
5. Search for: Hotel Description - Source Text, Amenities - Structured Tags, Owner Name, Operator / Management Company, Enrichment Status, Human Review Required, Production Use Status
6. Expect Enrichment Status = Not Started on rows; description/owner/operator/rooms/amenities cells blank by design
7. Do not expect renamed fields (Last Reviewed Date etc.) — renames were not applied

## Why founder may not see changes

- Looking at Deal Capture MVP or Sandbox instead of Deal Capture Platform
- Looking at Brand Setup - Brand Basics / Presentation instead of Hotel Property Census
- Looking at legacy Hotel Census or Verified Independent Hotel Census stub
- Expecting field renames from post-apply review — those were recommendations only, not applied
- Expecting filled description/owner/operator/rooms cells — blank until enrichment lane runs
- Wide table: new columns are toward the right of the original identity columns (scroll or field search)

## Recommended next step

Founder opens Deal Capture Platform → Hotel Property Census → Grid view → field search for Enrichment Status. Confirm 666 rows and blank enrichment columns. Then proceed to first enrichment lane (descriptions + amenities + property type) when ready.


## Scope

Read-only diagnostic. No Airtable writes, renames, deletes, or Brand Explorer patches.
