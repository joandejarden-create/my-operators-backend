# Findymail Contact Intelligence V1.1 — Canonical Airtable Architecture & Persistence Readiness

**Authorization:** GO — $0 schema design and dry validation  
**Date:** 2026-11-23  
**Prior:** `reports/gdi-findymail-contact-intelligence-preparation-v1/FOUNDER_REPORT.md`  
**Spend / Findymail API / schema apply / Surfe changes / publication / deploy:** **none**  
**Airtable classification:** `PERSISTENCE_DESIGNED` (not applied)  
**Status:** `STOP — awaiting founder schema / license / activation approvals`

---

## Executive verdict

Live schema on HPC base `appCCUsuGsE1ifoLk` **can** support durable Findymail emails **without** a Findymail Contacts table and **without** false hotel ownership:

1. **Person** = HI Org Contact affiliated to the **demand organization** (not the hotel).  
2. **Email** = HI Contact Channel (`Subject Type=Person`, `Kind=Email`).  
3. **GDI** keeps `personIdentityKey` + embedded `primaryContact*`; email is projected from the channel.  
4. **Hotel association** is via opportunity/`hotelId` links — **not** HI Hotel–Org Relationships for event stakeholders.

Missing for production-ready licensing/freshness: optional channel fields + a provider-agnostic usage-events table. **Findymail stays inactive.**

Zero-write certification: **15/15 PASS** (`ZERO_WRITE_CERTIFICATION.json`).

---

## 1. Schema compatibility findings (live read-only)

Evidence: `LIVE_SCHEMA_SNAPSHOT_READONLY.json` (Airtable Meta API, no writes).

| Table | ID | Present | Notes |
|---|---|---|---|
| HI Organizations | `tbliXEfUOWk32AN9y` | YES | Org types include `Other`, `Government`, … |
| HI Org Contacts | `tbldJAj0nK1l39JZL` | YES | Person × **Organization** affiliation |
| HI Contact Channels | `tblqC7C4ypy5Zojyw` | YES | Person/Org subject; Email/Telephone/LinkedIn/Other |
| HI Hotel–Org Relationships | `tblGiwN0foXXDESvZ` | YES | **Must not** be used for GDI demand stakeholders |
| GDI Surfe Usage Events | — | **NO** | Still proposed only |
| GDI Contact Enrichment Usage Events | — | **NO** | Not created |

### HI Org Contacts — required shape for identity

Live fields match `MAP_HI_ORG_CONTACT`: Person ID, Display Name, Organization ID + Organization link, Title, Affiliation Verified?, Person/Affiliation Dedupe Keys, Publication State, etc.

**Implication:** a durable HI person row **requires an organization affiliation**. That affiliation must be the **demand generator org**, not an invented hotel owner/operator link.

### HI Contact Channels — email SoT

Live fields include Provider, Observed At, Access / Publication Restriction (text), Verification Status (`Verified Direct` available), Person + Organization links, Dedupe Key.

**Absent today (confirmed):** Last Checked At · License / Retention Class · Retention Expires At · Provider Request ID · Credit Cost.

### GDI-only stakeholder question (Phase 1)

| Question | Finding |
|---|---|
| Can GDI-only stakeholders get a durable verified email? | **Yes**, via demand-org HI Org Contact + Person channel |
| Requires artificial hotel ownership? | **No** — forbid Hotel–Org Relationship creation for this path |
| Requires operator affiliation? | **No** |
| Second person identity system needed? | **No** — bridge `personIdentityKey` ↔ Person/Affiliation Dedupe Keys |
| If schema could not support? | Minimum extension would be optional Org Type values (`Association / Professional Body`, `Event Organizer`) — **not** a new Persons table. `Other` is already enough to start |

---

## 2. Recommended final person / channel model

```text
GDI opportunity stakeholder
  └─ personIdentityKey(name|org|domain)          [process identity]
  └─ HI Organizations (demand org, type Other/…) [optional durable org]
  └─ HI Org Contacts (person × demand org)       [durable person]
  └─ HI Contact Channels (Email on Person)       [durable email channel]
  └─ Opportunity + hotelId associations          [commercial context]
  └─ Usage Events (provider-agnostic, no PII)    [credits / dedupe]

Surfe  → session-only (unchanged)
Findymail → licensed channel provider (inactive until approvals)
```

**Binding rules**

- Email is never a person identity.  
- Missing email does not hide `customerFacingNamed` names.  
- Findymail success does **not** promote Sales Ready.  
- Do not create Findymail Contacts table.  
- Do not auto-create HI Hotel–Org Relationships for GDI event contacts.

Contract module:  
`lib/group-demand-intelligence/contact-intelligence/gdi-findymail-channel-persistence-contract-v1-1.js`

---

## 3. Exact optional Airtable changes (DO NOT APPLY)

### A. HI Contact Channels — field diffs (HPC base)

| Field | Type | Allowed / notes | Reuse alternative | Migration |
|---|---|---|---|---|
| **Last Checked At** | `dateTime` | ISO | Could overwrite `Observed At` (loses first-found) | Additive; backfill null |
| **License / Retention Class** | `singleSelect` | `PUBLIC_LISTED` · `PROVIDER_LICENSED_INTERNAL` · `PROVIDER_LICENSED_CUSTOMER_DURABLE` · `PROVIDER_LICENSED_EXPORT_OK` · `SUPPRESSED` · `OPT_OUT` · `EXPIRED` | Encode in `Access / Publication Restriction` text (weaker) | Additive |
| **Retention Expires At** | `date` | optional SLA | Notes only | Additive |
| **Provider Request ID** | `singleLineText` | internal audit | `Usage Provenance` text | Additive; **never customer-facing** |
| Credit Cost | `number` | optional | Prefer usage ledger | Optional / skip if ledger exists |

Permissions: INTERNAL_ONLY writers; customer APIs must not expose Provider Request ID.

Dedupe remains: existing **Dedupe Key** = `sha256(subjectId|email|value)`.

### B. Provider-agnostic usage ledger (recommended)

Prefer **one** table covering Surfe + Findymail + future providers (generalize Surfe V1.2 proposal), not a Findymail-only table.

| Item | Recommendation |
|---|---|
| Name | `GDI Contact Enrichment Usage Events` (or promote/rename Surfe proposal) |
| Base | Prefer **ADP** `appa2cE7FTRmIbB32` for GDI ops telemetry (Surfe plan) **or** HPC if IC wants graph adjacency — **IC decision** |
| Identity | `Lookup Event ID` (idempotent) |
| Fields | Provider · Outcome · Credits Charged · Cost Estimate USD · personIdentityKey **hash** · hotelId · opportunityId · Event At · Data Environment · Schema Version |
| **Forbidden** | email · phone · name · raw provider payload · API keys |

Until created: local FS ledger remains SoT (Surfe pattern).

### C. Optional Org Type select extensions (minimum, only if product wants clarity)

Add to HI Organizations.`Organization Type` (optional):  
`Association / Professional Body` · `Event Organizer` · `Corporate Account`  
Until then use **`Other`**.

### D. Explicitly not changing

- No Findymail Contacts table  
- No parallel GDI Persons table  
- No Surfe session policy changes  
- No live field deletes

---

## 4. Cross-hotel reuse implementation plan

1. Resolve `personIdentityKey` + demand-org affiliation keys.  
2. Cache order: local/durable index → HI channels by Person/Affiliation/email dedupe → other GDI opps with same key.  
3. Freshness: Last Checked At / Observed At ≤ **90 days** (founder-settable).  
4. License + suppression + customer-access gates.  
5. On hit: `LINK_OPPORTUNITY` for new hotel/opp; usage `REUSE_HIT` at **0 credits**.  
6. On miss: only if Findymail authorized → finder; else stop.  
7. Provider dedupe window (**30 days**) blocks repeat paid lookups for same identity+provider.

Certified in fixtures: same person → hotel_1 + hotel_2 → 0+0 credits.

---

## 5. Idempotent write contract (dry)

`planEmailChannelUpsert` stages:

1. Resolve person identity (demand org/domain required; hotel ownership forbidden).  
2. Verify employer/domain vs email domain.  
3. Check existing channels (dedupe + person+kind).  
4. Validate freshness.  
5. License / suppression / customer-access gates.  
6. Reuse if permitted.  
7. Else prepare authorized provider lookup (default **unauthorized** — Findymail inactive).  
8. Validate provider result (reject generics).  
9. Prepare upserts: HI Organization (demand) → HI Org Contact → HI Contact Channel → opportunity link → usage event.  
10. Preserve provenance (`Provider=Findymail`, request id, Observed/Last Checked).  
11. **Never** `UPSERT_HI_HOTEL_ORG_RELATIONSHIP` on this path.  
12. `salesReadyPromotion: false` always.

All ops marked `dryRun: true` until Persistence GO.

---

## 6. Provider-agnostic usage ledger

| Property | Value |
|---|---|
| Grain | One lookup/reuse decision event |
| PII | **None** |
| Providers | `findymail` · `surfe` · `public` · `reuse` · future |
| Outcomes | REUSE_HIT · LOOKUP_FOUND · LOOKUP_MISS · LOOKUP_SUPPRESSED · LOOKUP_STALE_REFRESH · LOOKUP_BLOCKED_* |
| Billing guard | Duplicate window + reuse path charge 0 |

Builder: `buildUsageLedgerEvent` (strips accidental email/name).

---

## 7. GDI email projection rules

| Rule | Behavior |
|---|---|
| Authorized verified name | Remains visible if `customerFacingNamed` even when email missing |
| Verified email | Sets `primaryContactEmail` / contact.email from channel |
| Customer-visible email | Only if publication permission **and** customer-capable license class |
| Tile / drawer | Name path independent of email (`#575` / V1.1 auth preserved) |
| Sales Ready | **Unchanged** by Findymail email verification |
| Surfe | Session-only; not a durable channel writer in this contract |

---

## 8. Licensing controls still requiring confirmation

| Control | Status |
|---|---|
| Commercial store/display/export green light (founder-asserted) | Design-ok; still need written Order/MSA note in-repo |
| Hotel-customer SaaS disclosure scope | **Open** |
| Retention / DSR deletion flow-down | **Open** |
| Geo / phone restrictions | Phone **OFF** for V1 |
| Redistribution / service-bureau limits | **Open** |
| Dealality suppression / opt-out list wiring | **Open** |
| Freshness SLA (default 90d) | Proposed; founder confirm |
| Customer display vs INTERNAL_ONLY default | Default **CUSTOMER_BLOCKED** until display GO |

---

## 9. $0 tests and results

```text
node scripts/test-gdi-findymail-contact-intelligence-v1-1.mjs
→ OK 15 zero-write certifications
```

Covered: HI reuse · GDI-only identity · cross-hotel reuse · duplicate lookup suppression · stale/generic email · license expiry · suppression · customer access · usage accounting without PII · upsert plan without hotel-org link · name without email · email without Sales Ready · Surfe untouched · live schema snapshot.

Artifacts:

- `ZERO_WRITE_CERTIFICATION.json`  
- `LIVE_SCHEMA_SNAPSHOT_READONLY.json`  
- Contract module + test script  

---

## 10. Separate founder approvals before next steps

| # | Approval | Unlocks |
|---|---|---|
| 1 | Confirm demand-org HI model (no hotel-org fabrication) | Implementation start |
| 2 | Schema GO — channel fields (± Org Type options) | `ensure` / field create |
| 3 | Schema GO — provider-agnostic usage-events table + base choice | Table create |
| 4 | Written licensing/retention/DSR/hotel-customer note | Durable Findymail PII |
| 5 | Findymail Basic subscription + credit ceiling | First API call |
| 6 | Persistence GO (dry-run → apply) | Live channel upserts |
| 7 | Customer display GO | Show Findymail email in GDI UI |
| 8 | Export/CRM GO | Downstream exports |
| 9 | Explicit no-Surfe-change confirmation | Keep session-only |
| 10 | IC coordination for shared HI graph fields | Cross-stream safety |

---

## Governance checklist

| Constraint | Status |
|---|---|
| No Findymail subscription / API | Observed |
| No Airtable schema mutation / live writes | Observed (meta read-only only) |
| No Surfe changes | Observed |
| No customer publication / export changes | Observed |
| No production deploy | Observed |
| Dirty worktrees preserved | Observed |

---

## STOP

Ready for founder review. **Do not apply schema or activate Findymail** until the approvals above are explicit.

Recommended next founder decisions (pick):

1. Approve demand-org person model as final.  
2. Approve/deny optional channel fields + usage-events table (and base).  
3. File written licensing note.  
4. Keep Findymail inactive until (1)+(2)+(3)+credit ceiling.
