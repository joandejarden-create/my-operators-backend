# GDI Contact Intelligence V2.3 — Founder Report

**Status:** Implementation + preflight complete · **STOP** before schema `--apply` and live contact writes  
**Baseline:** PR #558 merged at `bfbb921465a36700c2f638f7c64613a2059ec163`  
**Branch:** `feat/gdi-contact-intelligence-v2-3-person-index-pilot`  
**Persistence:** `PERSISTENCE_DESIGNED` · customer display **DENIED** · Surfe-first unchanged

---

## 1. Identity collision protection

Implemented `gdi-person-identity-collision-v1`:

- `personIdentityKey` (name|org hash) = **candidate grouping only**
- Second-stage: profile URL / person-specific source / fingerprint
- Same name+org **without** independent evidence → `AMBIGUOUS_UNRESOLVED` (no merge)
- Conflicting profiles/fingerprints → `DISTINCT_PERSONS` (branched durable keys)
- Matching source/profile evidence → `SAME_PERSON_CONFIRMED`
- Different orgs without continuity → distinct; continuity candidate does **not** transfer channels
- Durable store refuses ambiguous merges; tests cover explicit same-name/org collisions

## 2. Exact Airtable schema mapping and proposed changes

Live inspection (`appa2cE7FTRmIbB32`):

| Table | ID | Status |
|---|---|---|
| Group Demand Opportunities | `tblRuReslJMwsfRQj` | Reuse existing `primaryContact*` |
| GDI Hotel Research Opportunities | `tblbkDj0vhQLfrssV` | No typed contact columns |
| Demand Generators | `tblykUVOjGVkawvdD` | Org identity reuse |

**Existing GDO contact fields (reuse):**  
`primaryContactId/Name/Role/Email/Phone`, `contactQuality`, `opportunityPayloadJson`

**Proposed typed fields (all currently MISSING — do not apply yet):**

| Field | Type |
|---|---|
| personIdentityKey | singleLineText |
| contactSourceUrl | url |
| contactVerificationStatus | singleSelect |
| contactStorageAuthorization | singleSelect |
| contactCustomerDisplayPermission | singleSelect (`DENIED` for pilot) |
| contactRetentionRevalidationDays | number |
| contactIdentityDisambiguationFingerprint | singleLineText |
| contactLastVerifiedAt | dateTime |

No Contacts table. Schema `--apply` **not executed**. Packet: `SCHEMA_PROPOSAL.json`.

## 3. Durable person-index design

`EMBEDDED_PRIMARY_CONTACT_PLUS_DURABLE_PERSON_INDEX`

- Opportunity channels → GDO embedded columns
- Cross-week person SoT → durable fixture index with `groupingKey::fingerprint`
- Until typed fields land, index remains fixture/file-only (live Airtable person index off)

## 4–5. Eight-contact revalidation / eligible count

**Eligible: 8 / 8** (smallest subset = full set)

All pass identity, employer, opportunity link, verified channel, source URL, internal storage class, minimization, 365d retention, destination compatibility.  
Customer display: **DENIED**. Provider-derived: excluded.

## 6. Final transaction-level write manifest

`WRITE_MANIFEST.json` — allowlisted by `opportunityId`, redacted channel values, idempotency keys, rollback plan, fail-closed guards.  
`execute: false` · `liveWrites: false` · schema fields marked `FIELD_MISSING_PENDING_SCHEMA`.

## 7. Internal storage rights and retention

- Class: `INTERNAL_RESTRICTED_PUBLIC_SOURCE`
- Public accessibility ≠ display/export rights
- Retention/revalidation: 365 days
- Customer display: DENIED

## 8. Fixture create/update/read-back

`FIXTURE_PERSISTENCE_RESULTS.json` — create + cross-week durable reuse **PASS** for all 8.

## 9. Cross-week reuse tests

- Process-local vs durable layers distinguished
- Multi-hotel / multi-opportunity association
- Employer change, expiry, correction, deletion, audit
- Same-name collision branching
- Provider persistence refused

## 10. Privacy / security

- Live write flags hard-off
- Provider origins refused
- Reports redact contact values
- gitleaks PASS (`dealality:ci-precheck`)

## 11. PR and CI status

Focused PR opened for IC merge (see PR URL after open). Local tests PASS; CI precheck PASS.

## 12. Exact approvals remaining before live writes

1. **IC review** of exact GDO field names/types/options  
2. **Founder authorize schema `--apply`** (create-missing-only on GDO)  
3. **Founder authorize live-write token** for allowlisted `opportunityId`s only  
4. Confirm **customer display remains DENIED** for all pilot rows  
5. Resolve live Airtable `recordId` per opportunityId at execution (fail-closed if not unique)

---

**STOP** — no live contact writes, no unapproved schema mutation, no customer publication, no production deployment.
