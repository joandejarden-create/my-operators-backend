# GDI Contact Enrichment Airtable Schema V1 — Founder Report

**Authorization:** Founder GO — controlled additive Airtable schema creation  
**Date:** 2026-11-23  
**Safety:** No Findymail API · No Surfe API · No contact record writes · No synthetic usage rows · No publication

---

## 1. Confirmed GDI Airtable base

| Item | Value |
|------|-------|
| Base ID | `appa2cE7FTRmIbB32` |
| Role | AI Demand Positioning / GDI operational base |
| URL anchor table | `tblDwW2jpbKBBtpko` **Hotel Event Spaces** (confirms base identity) |
| GDI opportunities table | `tblRuReslJMwsfRQj` **Group Demand Opportunities** (verified) |
| Pre-create usage-like tables | **None** |

Fail-closed verification passed before mutation.

---

## 2. New usage-events table name and ID

| Item | Value |
|------|-------|
| Name | **GDI Contact Enrichment Usage Events** |
| Table ID | **`tblvUJlik63pETitY`** |
| Primary field | Lookup Event ID (`fldJn6IMVoqNDxX9B`) |
| Record count after create | **0** (no synthetic rows) |

---

## 3. Complete created field list and IDs

| Field | Type | ID |
|-------|------|----|
| Lookup Event ID | singleLineText (primary) | `fldJn6IMVoqNDxX9B` |
| Provider | singleSelect | `fldanrtIK5UEYCHEM` |
| Lookup Outcome | singleSelect | `fldf1OIMoW51E93K2` |
| Credits Charged | number | `fldRnU1K2D4L5jedD` |
| Cost Estimate USD | currency USD | `fldAMvuBbIG6UcYrj` |
| Person Identity Hash | singleLineText | `fldvw4DFnTfWX0gy1` |
| Hotel ID | singleLineText | `fldN9R6QRP3N0f0eS` |
| Opportunity ID | singleLineText | `fldA2aguKBXHhx1Y2` |
| Request Origin | singleSelect | `fldCtfG0PyCDvluPi` |
| Event At | dateTime | `fldKOSQkzFHyGrkKJ` |
| Data Environment | singleSelect | `fldfs7i6Zb6T9hMtp` |
| Schema Version | singleLineText | `fldaGStWc66Q5W1fw` |
| Provider Request Reference | singleLineText | `fld5BTA9c0oVyby3Q` |
| Notes | multilineText | `fldU5KTLXOd7cCpEp` |

**Provider choices:** Findymail · Surfe · Public Research · Reuse · Other  
**Lookup Outcome:** REUSE_HIT · LOOKUP_FOUND · LOOKUP_MISS · LOOKUP_SUPPRESSED · LOOKUP_STALE_REFRESH · LOOKUP_BLOCKED · LOOKUP_ERROR  
**Request Origin:** GDI Opportunity · Hotel Explorer · Contact Intelligence · Admin · Other  
**Data Environment:** Development · Staging · Production  

No email/name/phone/raw-payload/API-key fields.

---

## 4. HI Contact Channels additions and IDs

Base `appCCUsuGsE1ifoLk` · Table `tblqC7C4ypy5Zojyw` (identity verified).

| Field | Type | ID |
|-------|------|----|
| Last Checked At | dateTime | `fldyUP3ylAUP8uPUi` |
| License / Retention Class | singleSelect | `fldkJEwy47xROXmys` |
| Retention Expires At | date | `fldeIWS3vz8s4NyrE` |
| Provider Request ID | singleLineText | `fld4xwUDV3AQ2MBa8` |

License choices match Findymail V1.1 contract. **Credit Cost was not added** (costs live on usage ledger).

Existing Channel ID / Kind / Value / Verification / Publication fields unchanged.

---

## 5. Existing schema compatibility

- No rename/delete of existing tables or fields
- No duplicate usage table (Surfe-only table did not exist; not created)
- Canonical contact model preserved: Opportunity → personIdentityKey → HI Org → HI Org Contact → HI Contact Channel
- No HI Hotel–Org Relationships for demand contacts
- No Findymail Contacts table

---

## 6. Idempotency and deduplication contract

- **Lookup Event ID** is the upsert identity for usage events (hash of provider + personIdentityHash + opportunityId + at + outcome when not supplied)
- Channel dedupe remains subject + kind + normalized value (`channelDedupeKey`)
- Provider re-bill suppression window remains 30 days in Findymail V1.1 planner (code-level; no live lookups)
- Ensure script is idempotent: second `--apply` skipped all fields

---

## 7. Provider usage accounting design

One shared ledger for Findymail, Surfe, Public Research, Reuse, Other.

- Credits + Cost Estimate USD on usage events only
- Opaque Person Identity Hash (never readable PII)
- Request Origin + Data Environment for attribution
- Surfe sync module retargeted to this table ID; dry-run default retained; durable Surfe email/phone still forbidden

---

## 8. Read-back verification results

| Check | Result |
|-------|--------|
| Usage table exists with 14 required fields | PASS |
| Select choices match founder list | PASS |
| No forbidden PII field names | PASS |
| HI four enrichment fields present | PASS |
| Existing HI field IDs preserved (Channel ID `fldSPz6EXrA6FUO6E`, etc.) | PASS |
| Usage record count | **0** |
| HI Contact Channels record count | **6** (unchanged by schema ops) |
| Idempotent re-apply | PASS (all skipped) |

Evidence: `APPLY_REPORT.json`, `TEST_RESULTS.json`.

---

## 9. Confirmation — no contact records changed

Schema Meta API only (`POST /meta/.../tables`, `POST .../fields`).  
No `PATCH`/`POST` to HI Contact Channels records.  
Usage table empty. Sample list fingerprint unchanged across ensure.

---

## 10. Confirmation — Findymail and Surfe policies unchanged

| Policy | Status |
|--------|--------|
| Findymail subscription / API | **Inactive** |
| Findymail enrichment / email persistence | **Off** |
| Surfe explicit-click workflow | **Unchanged** |
| Surfe-returned email/phone durable storage | **Still forbidden (session-only)** |
| Usage ledger may track Surfe costs without PII | **Schema ready; sync disabled** |

---

## 11. PR and CI status for code changes

Code/docs/tests live on branch `feat/gdi-contact-enrichment-airtable-schema-v1` (isolated worktree).  
Includes:

- `lib/.../gdi-contact-enrichment-airtable-schema-v1.js` (live IDs)
- `scripts/ensure-gdi-contact-enrichment-airtable-schema-v1.mjs`
- Findymail V1.1 contract alignment + destinations
- HI `MAP_HI_CONTACT_CHANNEL` enrichment fields
- Surfe usage proposal retargeted to shared ledger
- Docs + regression tests

**PR:** to be opened · Integration Controller owns merge · `ws-gdi` dirty work preserved

---

## 12. Remaining activation approvals

Still require separate founder GO:

1. Findymail subscription + API enablement + credit ceiling  
2. Findymail → HI Contact Channels persistence apply (dry-run → apply)  
3. Surfe usage Airtable sync enable (`GDI_SURFE_USAGE_AIRTABLE_SYNC_ENABLE=1`) without PII  
4. Customer email display / export  
5. Weekly automation  
6. Any production deployment  

### Cycle state

**`AWAITING_INTEGRATION_CONTROLLER`** (after PR open) for code merge.  
**Airtable schema itself is already live** under this founder GO.
