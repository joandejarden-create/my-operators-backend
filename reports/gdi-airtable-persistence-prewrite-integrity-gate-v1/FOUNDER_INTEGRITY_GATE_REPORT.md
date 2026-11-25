# Pre-Write Integrity Gate — Bethesda Research Persistence

**Verdict: READY_FOR_CANARY**

PR **#599** amendment evidence. Production bulk writes **not** authorized. Canary **not** executed.

---

## Task 1 — Commercial eligibility mapping

**Verdict:** `CORRECT_GLOBAL_ORG_CLASSIFICATION_SEPARATE_FROM_HOTEL_FQO`

Typed Airtable `commercialEligibilityStatus` is Full E2E **write-routing** (Demand Generators vs GDO), **not** hotel FQO.

| Observation | Result |
|---|---|
| All unique inventory rows | `GLOBAL_ORG_ONLY` |
| Why | `HOTEL_OPPORTUNITY` always allows Demand Generators → `GLOBAL_ORG_ONLY` unless GDO gates also pass |
| GDO gates | Confirmed/reliable `demandThesis` **enum** + credible commercial entry point |
| Why GDO deferred | STAGED/package payloads carry narrative `hotelOpportunityThesis`, rarely DEMAND_THESIS enums |

Hotel-specific FQO is retained separately in:

`researchPayloadJson.hotelSpecificQualification`

Certified STAGED 68 (unique):

- Named contact: **68/68**
- FQO pass (historical-compatible evaluation): **68/68**
- Disposition / research continuation / disqualified rows remain distinguishable via manifestDisposition + lifecycle

Do **not** force hotel FQO labels into `commercialEligibilityStatus`.

Evidence: `01-COMMERCIAL_ELIGIBILITY_MAPPING.json`, `04-PAYLOAD_ROUNDTRIP_CERTIFICATION.json`

---

## Task 2 — Audit of the prior 85 updates

Prior dry-run treated package replace as 85 updates (payload / dgGeneratorId / sourceSummary / title / segment / `lastMaterialChangeAt`).

### Code fixes applied

1. **`lastMaterialChangeAt` removed from `MATERIAL_RESEARCH_FIELDS`** — stamp only when another material field changes.
2. **Lossless live merge** (`mergeProposedResearchInventoryFields`) — live contacts, outreach, org names, and HTTP evidence are unioned; package cannot wipe live research.
3. **Contact dedupe key** uses name (+ email), so co-contacts sharing an org inbox (e.g. Justin Fessler + Andrea Snader) are retained.
4. **Title corruption guard** — keep richer live titles when proposed shortens or corrupts en-dashes.
5. **FQO block** added to slim payload for retrieval.

### Revised write picture

| Class | Count |
|---|---:|
| WOULD_CREATE | 1 |
| WOULD_UPDATE (material after lossless merge) | 85 |
| HOLD / possible loss | 0 |
| Duplicate aliases (no separate write) | 3 |
| Research continuation identities | 4 |
| Disqualified / historical identities | 11 |

Every remaining update includes a `researchPayloadJson` change driven by slim reshape + **`hotelSpecificQualification` overlay** (required for lossless FQO retrieval). Additional fills: `dgGeneratorId` (59), `sourceSummary` URL union (45), conservative title/segment normalization.

**Not** authorized as a bulk production run — canary first.

Evidence: `03-UPDATE_FIELD_AUDIT.json`

---

## Task 3 — Payload round-trip certification

Representative categories in `04-PAYLOAD_ROUNDTRIP_CERTIFICATION.json`:

- Certified candidate
- Research continuation
- Historical exclusion
- Duplicate/merged family (alias mapping)
- New record (create candidate)

Round-trip failures: **0**. Field-size refusals: **0** (fail-closed if over limit).

---

## Task 4 — Live Airtable canary (prepared only)

| Item | Value |
|---|---|
| Status | `AWAITING_FOUNDER_APPROVAL` |
| Executed | **false** |
| Preferred identity | `gdi_e2d_770e3b44ad97c1ae` |
| Action | CREATE (no existing Airtable row) |
| Table | GDI Hotel Research Opportunities (`tblbkDj0vhQLfrssV`) |
| Max writes | **1** |

Verify after founder GO: create → read-back → unchanged rerun → idempotency → duplicate protection → no GDO / STAGED / CURRENT side effects → reversible via pre-write snapshot.

Evidence: `05-CANARY_PREPARATION.json`

---

## Task 5 — Revised production manifest

`06-REVISED_PRODUCTION_MANIFEST.json` reconciles all **88** original Bethesda research identities (canonical writes + duplicate aliases).

Objective is accuracy and losslessness — not retaining an 86-write target. Suggested post-canary budget remains founder-gated.

STAGED quality 68 / CURRENT gold 67 protected (`07-STAGED_CURRENT_INTEGRITY.json`).

---

## Required verdict

# READY_FOR_CANARY

Data integrity verified under read-only Airtable comparison + lossless merge + mock/unit proof.  
Exact limited Airtable canary identity ready for founder approval.  
**Do not** claim production persistence certification from mock read-back alone.
