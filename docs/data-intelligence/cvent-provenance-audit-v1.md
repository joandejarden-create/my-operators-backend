# Cvent Provenance Audit v1

**Status:** `production_census_cvent_provenance_audit_v1_complete`  
**Objective:** `cvent-provenance-audit-v1`  
**Generated:** 2026-08-09T17:04:41.051Z  
**Table:** Hotel Property Census only  
**Countries:** Dominican Republic, Costa Rica, Panama

## Counts
- Total shell records reviewed: **1194**
- Cvent-only: **256**
- Cvent + HBX: **144**
- HBX-only: **794**
- Independent / unknown: **0**

## Trackability
- Cvent-origin rows with usable provenance trail: **400** / **400**
- Rows missing Cvent provenance: **0**
- Rows incorrectly using Cvent as validation: **0**
- Cvent-only with Current Brand / Brand Family populated: **0**

## Confirmations
- Cvent-only marked `Cvent Candidate / Not Field Source`: **true**
- Cvent+HBX marked Multi-Source / Cvent+HBX discovery: **true**
- No row says "Validated by Cvent": **true**
- Cvent not used as field-level SoT (Discovery Source wording): **true**
- Current Brand / Brand Family not from Cvent-only: **true**

## Issue breakdown
- none

## Patch plan
- Rows needing patch: **0**
- none required


## Samples
### Cvent-only
- dreams royal beach punta cana all ages all inclusive | Cvent Candidate / Not Field Source | Cvent Identity Candidate | mix=cvent_candidate
- dominican fiesta hotel casino | Cvent Candidate / Not Field Source | Cvent Identity Candidate | mix=cvent_candidate
- sunscape coco punta cana all ages all inclusive | Cvent Candidate / Not Field Source | Cvent Identity Candidate | mix=cvent_candidate
- tropical beach club | Cvent Candidate / Not Field Source | Cvent Identity Candidate | mix=cvent_candidate
- solarium coronado beach | Cvent Candidate / Not Field Source | Cvent Identity Candidate | mix=cvent_candidate

### Cvent + HBX
- hotel cuna del angel | Cvent + HBX Candidate | Multi-Source Candidate
- albrook inn | Cvent + HBX Candidate | Multi-Source Candidate
- drake bay getaway resort | Cvent + HBX Candidate | Multi-Source Candidate
- catalonia bayahibe | Cvent + HBX Candidate | Multi-Source Candidate
- hotel 1492 | Cvent + HBX Candidate | Multi-Source Candidate
