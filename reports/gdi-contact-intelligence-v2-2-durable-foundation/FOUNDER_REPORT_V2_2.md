# GDI Contact Intelligence V2.2 — Founder Report

**Status:** Implementation complete · PR ready · **STOP** before schema mutation / live persistence / customer publication / production deploy  
**Base:** `origin/main` (includes merged PR #554 @ `653fd98fd`)  
**Branch:** `feat/gdi-contact-intelligence-v2-2-durable-foundation`  
**Persistence classification:** `PERSISTENCE_DESIGNED` (writes still DISABLED)

---

## 1. Safe workspace reconciliation

PR **#554 MERGED** at `653fd98fd` (verified ancestor of `origin/main`).

All registered product/IC/staging checkouts are **dirty and/or behind**. No reset/clean/force-FF performed.

V2.2 developed in exceptional clean WT from `origin/main`. Details: `WORKSPACE_RECONCILIATION.md`.

Registered `ws-gdi` remains on dirty feature branch — **preserved**.

## 2. Recommended canonical contact data model

**Strategy:** `EMBEDDED_PRIMARY_CONTACT_PLUS_DURABLE_PERSON_INDEX`

| Entity | SoT |
|---|---|
| Person | `personIdentityKey` + durable person index (fixture/file until Airtable fields approved) |
| Organization | Demand Generators + opportunity organization fields |
| Employment | Embedded role/title + employer-change history in durable index |
| Email / phone | GDO `primaryContact*` + research inventory payload/typed fields |
| Evidence / provenance | `contactSourceUrl` / evidence[] + field-provenance module |
| Rights / retention | Durable index rightsMetadata (typed Airtable rights pending) |
| Opportunity / hotel | Existing GDO + research inventory hotel-scoped rows |
| Verification history | Provenance supersession + durable audit log |

**Do not create a Contacts table now** — prior Slice-2 audit and V2.1 dry-run agree embedded fields can carry channels; durable person index closes cross-week/multi-hotel reuse without a competing DB.

## 3. Existing Airtable destinations reused

- Group Demand Opportunities (GDO) — typed `primaryContact*`
- GDI Hotel Research Opportunities — research SoT mirror
- Demand Generators — organization identity
- Embedded provenance / source URL in payload

## 4. Justified schema additions (founder approval required)

| Addition | Required for live pilot? |
|---|---|
| `personIdentityKey` on GDO + research inventory | Yes |
| `contactSourceUrl` / `contactVerificationStatus` typed | Yes |
| `contactRightsRetentionDays` / display permission flags | Yes |
| Normalized Contacts table | **No** — only if pilot proves embedded+index insufficient |

No schema mutations in this PR.

## 5. Cross-week reuse design and tests

Layers:

1. **PROCESS_LOCAL_CACHE** — existing workflow Map (same process / multi-hotel)
2. **DURABLE_FIXTURE_STORE** — file-backed person index (cross-week tests; live Airtable off)

Tested behaviors (`test:gdi-contact-intelligence-v2-2-durable-foundation`):

- Cross-hotel process-local reuse vs durable cross-week reuse (layers distinguished)
- Create / read-back / update / dedupe
- Cross-opportunity + cross-hotel association without duplicate persons
- Employer change → prior key invalidated, channels cleared (re-verify required)
- Expiry / correction / deletion / audit history
- Provider-origin persistence refused

## 6. Persistence eligibility — 39 hypothetical contacts

From V2.1 hypo set (39):

| Review | Count |
|---|---:|
| Deeper pilot-eligible excl. live-write **and** customer display | **34** |
| Missing evidence URL | 1 |
| Weak employer / identity | 1 |
| Customer display auto-granted | **0** (explicitly refused) |

Public availability ≠ export/display rights.

## 7. Eight pilot candidates — detailed readiness

All **8/8** pass every requirement other than:

- `LIVE_WRITES_DISABLED`
- `authorizedCustomerVisibilityExplicit` (founder must opt-in per channel/use)

| # | Name | Organization |
|---|---|---|
| 1 | Adam Grosswirth | National Alliance for Musical Theatre |
| 2 | Josh Cacchione | The Broadway League |
| 3 | Larry Schiffer | ARIAS U.S. |
| 4 | Mary Cunningham | CAME |
| 5 | Katie Hollis | SIOR |
| 6 | Jennifer Kearney | NYSSBA |
| 7 | Michele Smith Koontz | SSSP |
| 8 | Maritza Jeanette Medina García | Gobierno de Jalisco / SIOP |

Evidence URLs and public provenance verified via opportunity load + readiness gates.

## 8. Fixture-based persistence results

Durable store simulation: **PASS** (create, read-back, update, dedupe, cross-opp/hotel, cross-week, employer change, expiry, correction, deletion, audit).  
Live Airtable writes: **none**.

## 9. Privacy / licensing / retention blockers

- Provider-derived channels (Surfe/Findymail/PDL/Prospeo/FullEnrich) never durable-persisted
- Retention default 365d + revalidation; stale evidence expires
- Customer display requires explicit founder/auth flag (not inferred from public web)
- No production table/field creation in this PR

## 10. Integration and CI

Local:

- `test:gdi-contact-intelligence-v2-2-durable-foundation` PASS (9)
- Candidate validation script PASS
- `dealality:ci-precheck` (run at PR time)

Hand-off: Integration Controller merges after CI. GDI does not merge.

## 11. Exact founder approvals required for controlled live pilot

1. Authorize optional typed fields: `personIdentityKey`, `contactSourceUrl`, `contactVerificationStatus`, rights/display flags on GDO and/or research inventory  
2. Authorize `GDI_PUBLIC_CONTACT_LIVE_WRITE_ENABLED` for **fixture/authorized 8-contact subset only**  
3. Authorize customer-display permission per contact (or explicit pilot allowlist)  
4. Confirm destination = embedded `primaryContact*` (no Contacts table)  
5. Confirm Surfe-first waterfall unchanged; Findymail/PDL/FullEnrich remain OFF  
6. Authorize post-merge workspace sync plan for dirty product homes (IC)

---

**STOP** — no schema mutation, live contact persistence, customer publication, or production deployment without the approvals above.

**Final product state after PR open:** `AWAITING_INTEGRATION_CONTROLLER`
