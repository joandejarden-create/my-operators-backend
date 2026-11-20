# Bethesda Marriott — Phase 3 Intelligence Completion · Founder Review

**Hotel:** Bethesda Marriott · HPC `recLuxvwwxID7U2B8` · Base `appCCUsuGsE1ifoLk`  
**HEAD:** `8a218a45fb47b5be4213fd1e6b35b47f22202a21` (`ws/hotel-explorer` = `origin/main`)  
**Status:** Research complete · **STOP before production Airtable apply** · await founder authorization  
**Spend:** **$0.00** of $25 authorized (W0 free-first sufficient; W1/W2 unused)  
**KGPV:** UX gold only — not treated as verified intelligence

---

## 1) Baseline revalidation (mutually exclusive 63-field accounting)

Live Gate 1 metrics revalidated on merged main / `:8091`:

| Exclusive reporting category | Count | Definition |
|---|---:|---|
| **LIVE_AIRTABLE_TO_UI** | **42** | `liveIntegrated=true` — verified HPC/HI → UI |
| **GENUINE_RESEARCH_GAP** | **16** | Canonical intelligence missing after reuse |
| **UI_POPULATED_UNVERIFIED_SOURCE** | **5** | UI text present without verified HPC/HI attribution (`what_remains_uncertain`, `corporate_contact_channels`, `research_gaps`, `organization_contacts`, `research_unresolved`) |
| **TOTAL** | **63** | 42 + 16 + 5 = 63 — no fields dropped |

Also confirmed: integration gaps / not-connected = **0**; no Validated Economic Owner before this package; HI Org Contacts / Channels for ownership = **0** before proposed writes.

---

## 2) Economic Owner / PropCo / UBO findings

| Role | Entity | Status | Confidence | Notes |
|---|---|---|---|---|
| **Legal Owner / PropCo** | **Marbeth Acquisition LLC** | **VERIFIED** | HIGH | SDAT owner of record, Hotel Unit, acct `07-03547343` |
| **Economic Owner (sponsor)** | **Mullen Real Estate Capital** | **VERIFIED** (sponsor attribution) | MEDIUM–HIGH | Active portfolio listing + HQ mailing match to PropCo; WBJ related headline “sold to Mullen family” |
| **UBO / natural-person control** | — | **UNRESOLVED** | — | Private LLC; Jeff Mullen is decision authority, not proven UBO of record without entity ownership filings |
| Brand / Operator | Marriott International / Marriott Hotel Services, LLC | Existing HI edges | HIGH | **Not** economic owner |

### Title / transfer chronology (SDAT)

| Date | Seller → Buyer (current owner after transfer) | Price | Type |
|---|---|---:|---|
| 2013-10-10 | CMG Bethesda Owner LLC → (Northview/Oaktree era title) | $35,636,141 | Arms-length |
| 2017-09-27 | Bethesda Hotel Acquisition LP → Bethesda Hotel Acquisition LLC | $0 | Non-arms-length |
| **2019-01-09** | **Bethesda Hotel Acquisition LLC → Marbeth Acquisition LLC** | **$69,500,415** | **Arms-length** · Deed `/57084/ 00234` |

PropCo mailing on SDAT: **7 Campus Blvd, Newton Square PA 19073** — same campus as [Mullen Real Estate Capital](https://mullencap.com/contact/) (Newtown Square spelling).

Adjacent Exeter / residential-tower coverage at “5151 Pooks Hill” refers to **development parcel**, not the Hotel Unit condominium — excluded from ownership claim.

---

## 3) Verified people and contact channels (owner-side)

| Person | Title | Role class | Status | Source |
|---|---|---|---|---|
| Jeff Mullen | President and CEO | Decision authority | VERIFIED | [mullencap.com/about](https://mullencap.com/about/) |
| Katherine Button | VP, Asset Management and Investments | Asset management leadership | VERIFIED | same |
| Patrick Reece | Director, Asset Management | Relevant entry point | VERIFIED | same |

| Channel | Value | Status |
|---|---|---|
| Org email | `info@mullencap.com` | VERIFIED |
| Org HQ | 7 Campus Blvd., Suite 300, Newtown Square, PA 19073 | VERIFIED |
| Personal emails / mobiles | — | **RESEARCH_REQUIRED** (no paid W2 spend; stop condition met without individual enrichment) |
| Qualified signatories | — | **UNRESOLVED** (no authority instrument found) |

Operator-side SAM names (Jenny Pyne, Shreen Mann, Debi Spinks) are **hotel operations / gov sales**, not ownership decision-makers — not promoted as ownership contacts.

---

## 4) All 16 research gaps — field matrix

| # | Field | Status | Finding (summary) | Airtable destination |
|---|---|---|---|---|
| 1 | `economic_owner` | **VERIFIED** | Mullen Real Estate Capital (sponsor); PropCo Marbeth | HI Orgs + Rel `Economic Owner` + Evidence |
| 2 | `property_company` | **VERIFIED** | Marbeth Acquisition LLC | HI Orgs + Rel `Legal Owner` + Evidence |
| 3 | `ubo_group` | **UNRESOLVED** | Controlling org MREC; natural-person UBO not established | Evidence note / leave blank edge |
| 4 | `decision_authority.people` | **VERIFIED** | Jeff Mullen (CEO); AM: Button, Reece | HI Org Contacts |
| 5 | `qualified_signatories` | **UNRESOLVED** | No public signing-authority proof | — |
| 6 | `people_groups` | **VERIFIED_DERIVED** | Ownership / AM groups from MREC roster | HI Org Contacts (role_group) |
| 7 | `emails_phones` | **VERIFIED** (org) / **RESEARCH_REQUIRED** (personal) | Org email + address only | HI Contact Channels |
| 8 | `key_people` | **VERIFIED** | Jeff Mullen; Button; Reece on owner org | HI Org Contacts |
| 9 | `asset_agreement_profile` | **VERIFIED_DERIVED** | Marriott continues as manager (HWE 2018); MHM Ops & Investment Supervision; HPC Management Model already live | Evidence (+ optional Rel note); **no new franchise invention** |
| 10 | `property_history` | **VERIFIED** | 2013 $35.6M; 2019 $69.5M to Marbeth; Northview historical | HI Graph Evidence + Run |
| 11 | `development_pursuit` | **NOT_APPLICABLE** | Open operating hotel; no material owner-side pursuit package found | Run note only |
| 12 | `source_coverage` | **VERIFIED_DERIVED** | 16 evidence rows registered | Evidence inventory + Run |
| 13 | `what_we_know[]` | **VERIFIED_DERIVED** | Derived from validated edges + evidence (persist facts, not prose dump) | Derived read-model |
| 14 | `sources` | **VERIFIED_DERIVED** | Evidence register URLs | HI Graph Evidence |
| 15 | `starRating` | **VERIFIED_DERIVED** | ~4★ OTA consensus (Trivago/Google); not AAA cert retrieved | HPC steward optional |
| 16 | `projectPhase` | **NOT_APPLICABLE** | Radar/HPC Open asset | Leave blank |

---

## 5) Evidence register & source quality

Full register: `reports/hotel-explorer/phase3-research/HE-BETH-PHASE3-EVIDENCE-REGISTER-V1.json`

| Quality tier | Sources |
|---|---|
| **Authoritative (primary)** | MD SDAT owner + transfers (acct 03547343) |
| **Strong commercial** | MREC / MHM official portfolio & team pages; HWE sale release; Hotel Online 2013 |
| **Corroborative** | WBJ related headline; Ramsfield financing page; LoopNet APN |
| **Weak / excluded for ownership** | Brand parent, operator SAM alone, Exeter residential parcel news |

---

## 6) Research expenditure

| Wave | Scope | Spent | Outcome |
|---|---|---:|---|
| W0 | Free SDAT + public web | **$0** | PropCo + economic sponsor + history + org contacts |
| W1 | Paid ownership (≤$15) | **$0** | Not needed |
| W2 | Paid contact enrichment (≤$10) | **$0** | Org channels sufficient; personal emails deferred |
| **Total** | Cap $25 | **$0** | **$25 remaining unused** |

Provider tracking for HI Ownership Research Run: `MD_SDAT_Real_Property` $0; `public_web_free` $0.

---

## 7) Airtable write-back dry-run (**STOP — no apply**)

Artifact: `reports/hotel-explorer/phase3-research/HE-BETH-PHASE3-HI-GRAPH-DRY-RUN-V1.json`  
Planner: `planGraphUpserts` against empty index (idempotent on apply against live index).  
`applyAuthorized: false` · `resolveGraphDataApply` remains founder-gated.

| Table | Create | Update | Unchanged | Held / rejected |
|---|---:|---:|---:|---:|
| HI Organizations | 3 (Marbeth, MREC, MHM) | 0 | Existing Marriott org | 0 |
| HI Hotel–Org Relationships | 2 (`Legal Owner`, `Economic Owner`) | 0 | Existing Brand + Operator | 0 |
| HI Org Contacts | 3 | 0 | — | 0 |
| HI Contact Channels | 2 | 0 | — | 0 |
| HI Ownership Research Runs | 1 | 0 | — | 0 |
| HI Graph Evidence | *(manual/steward pack from register — not auto-ops in planner)* | — | — | — |
| HPC | 0 (starRating optional steward) | 0 | All existing fields | — |

**Would be left unchanged:** Brand/Operator Marriott relationships; all live HPC Overview fields; GET-only APIs.  
**Would be rejected/held:** none in this dry-run.  
**Would be linked:** Hotel ↔ Marbeth (Legal Owner); Hotel ↔ MREC (Economic Owner); contacts ↔ MREC.

---

## 8) Proposed five-tab improvement (preview only — **not live from Airtable**)

Do **not** treat the following as persisted UI truth until apply + retrieval proof.

| Tab | Before | After authorized persist (proposed) |
|---|---|---|
| Ownership | Unverified owner; historical Northview only staged | Legal Owner Marbeth; Economic Owner MREC; history 2013/2019; evidence pack |
| Organization | Brand/operator Marriott only | + MREC / MHM orgs; owner-side key people |
| Contacts | Sparse / unverified UI stubs | Jeff Mullen, Katherine Button, Patrick Reece; org email + HQ |
| Hotel Overview | Already strong HPC live | Optional 4★ derived; project phase remains N/A |
| Relationships viz | Brand + Operator | + Legal Owner + Economic Owner hierarchy (Marriott ≠ owner) |

Remaining unresolved after package: natural-person UBO; qualified signatories; personal emails/phones; AAA/official star certificate.

---

## 9) Outstanding gaps & limitations

1. SDAT result page URL is session/search-bound — cite **account 07-03547343** + deed `/57084/ 00234` + observation date.
2. WBJ full “sold to Mullen family” article body not retrieved (paywall) — headline only.
3. UBO natural persons not proven — do not invent.
4. Evidence table rows need stewarded insert alongside planner ops (planner creates orgs/rels/people/channels/run).
5. Dirty HE worktree preserved; Phase 3 artifacts are local reports — commit/PR only if founder wants them on `main` before apply.
6. `parallel-cli` unavailable locally; research used free SDAT interactive + public web ($0).

---

## 10) Recommended refresh / dedupe for subsequent hotels

1. **Always start with assessor/SDAT (or equivalent)** for Legal Owner before trade press.  
2. Match PropCo mailing → sponsor HQ before asserting Economic Owner.  
3. Keep Brand/Operator edges separate; never auto-fill Economic Owner from brand.  
4. Idempotent `orgDedupeKey` / relationship dedupe via existing planner — no name-only merges.  
5. Spend W2 only after owner org identity is Validated and org channels are insufficient.  
6. Persist Evidence + Research Run cost ledger every wave even when spend = $0.

---

## Founder decision required

**Approve production Airtable apply** for the dry-run package (or request edits) before any write.  
No production deploy. No competing research store. No KGPV fixture migration.

**Airtable persistence classification after apply (expected):** `PERSISTENCE_VALIDATED` only after dry-run apply + retrieval proof; currently **`PERSISTENCE_DESIGNED`** + research package ready.
