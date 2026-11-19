# HE-INTEL-001 P2 — Founder completion report

**Status:** `AWAITING_INTEGRATION_CONTROLLER` — STOP before merge  
**Branch:** `feat/he-intel-001-p2-airtable-intelligence-graph`  
**Base P1 merge:** `0b867c43c394e40cdbf1d8780fb01f692bcbe1d5`  
**Workspace:** `C:\Dev\dealality\ws-hotel-explorer`  

## 1. Airtable IDs (base `appa2cE7FTRmIbB32`)

| Table | ID | Status |
| --- | --- | --- |
| HI Organizations | `tbl4XhanPXiUIsvaB` | **CREATED** |
| HI Hotel–Org Relationships | `tblbBbIYtIWEPyRPH` | **CREATED** |
| HI Org Contacts | `tblki7hgPmcrkrfSY` | **CREATED** |
| HI Contact Channels | `tblMscC8FgXUpmFts` | **CREATED** |
| HI Ownership Research Runs | `tbl6cUPY5J84P3QLn` | **CREATED** |
| Hotel Intelligence Evidence | `tblHxS8x0niDEGVsh` | **REUSED** |

Links created: Parent Organization (self), Relationship→Org, Contact→Org, Channel→Person, Channel→Org.

## 2. Created vs reused

- **Created:** five approved graph tables + link fields  
- **Reused:** Hotel Intelligence Evidence  
- **Deferred (manual):** Evidence `Entity Type` option append — Meta API returns “Changing a field's type…”. Add manually: Organization, Hotel–Org Relationship, Org Contact, Contact Channel, Ownership Research Run. Do **not** recreate the field.

## 3. Entity model

Organizations (`dle_`) · Relationships (`dor_`) · Persons (`dlp_`) · Channels (`dlc_`) · Research runs (`ownrun_`). Brand / operator / legal owner are distinct relationship types. Person identity ≠ affiliation. Publication layers separated; Airtable persist ≠ customer publish.

## 4. Migration / rollback

- Idempotent ensure script: `--dry-run` then `--apply`  
- Non-destructive (no deletes/renames)  
- Rollback: leave empty tables; do not auto-drop. Data apply remains gated off.

## 5. Tests

`npm run test:he-intel-001-p2-airtable-graph-v1` — **14/14 PASS**  
`npm run test:he-intel-001-research-engine-v1` — **24/24 PASS**  
`dealality:ci-precheck` — PASS  

## 6. Dry-run vs apply (data)

Synthetic store: dry-run writes=0; apply requires founder auth + synthetic allow; bulk production writes **disabled**.

## 7. Security / publication

Customer publication blocked. HPC Owner/Operator overwrite forbidden. Radar Status mutation forbidden. Name-only merge forbidden.

## 8. Remaining before P3

- Manual Evidence Entity Type choices  
- Production data apply path (founder-gated) after validated research  
- P3 paid research-method benchmark  

## 9. WIP preservation

ADP revoked-token registry, HE-INTEL evidence/reports, tmp probes **not** committed.

---

**STOP** — no merge, no bulk research apply, no paid research, no deploy, no customer publication.
