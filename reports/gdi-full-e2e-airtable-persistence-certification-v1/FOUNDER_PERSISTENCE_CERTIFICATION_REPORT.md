# Canonical GDI Full E2E — Airtable Research Persistence Certification

**At:** 2026-11-24T23:56Z  
**Hotel reference:** Bethesda Marriott (`recLuxvwwxID7U2B8`)  
**Certified STAGED:** `gdi_hotel_wide_fqo_recLuxvwwxID7U2B8_20261125_quality_v1` (68)  
**Customer CURRENT:** `gdi_pub_recLuxvwwxID7U2B8_20261113_local_master_v2` (67) — **unchanged**  
**Production Airtable writes:** **NOT EXECUTED**

---

## 1. Existing persistence architecture

Reused (not duplicated):

| Module | Role |
|---|---|
| `gdi-canonical-full-e2e-entry-v1.js` | Optional research-persistence stage after research; hotel-wide finalization remains separate |
| `gdi-full-e2e-research-persistence-bridge-v1.js` | Dry-run / gated apply into research inventory |
| `gdi-hotel-research-inventory-store-v1.js` | Composite-key upsert + material diff + read-back |
| `gdi-full-e2e-airtable-persistence-contract-v1.js` | Base/table routing + production write guard |
| `hotel-research-opportunity-field-map.js` | Field map + lifecycle/eligibility enums |

**Hard separations preserved:** research inventory ≠ GDO ≠ STAGED/CURRENT.

## 2. Live Airtable schema compatibility

- Base: **AI Demand Positioning** `appa2cE7FTRmIbB32`
- Table: **GDI Hotel Research Opportunities** `tblbkDj0vhQLfrssV`
- Live field count: **18**
- Mapped fields missing: **none**
- Evidence: `01-LIVE_SCHEMA_COMPATIBILITY.json`

## 3. Research → Airtable field mapping

Typed columns: `compositeKey`, `opportunityId`, `hotelId`, `organizationName`, `researchLifecycleStatus`, `commercialEligibilityStatus`, `demandThesis`, `priority`, `title`, `segment`, `sourceSummary`, discovery/research timestamps, `dgGeneratorId`, `schemaVersion`.

Structured contacts, evidence URLs, FQO/disposition metadata ride in **`researchPayloadJson`** (slim commercial allowlist). Surfe-returned details are not stored.

Evidence: `08-RESEARCH_TO_AIRTABLE_FIELD_MAPPING.json`

## 4–5. Bethesda 88 dry-run write manifest

| Metric | Count |
|---|---:|
| Manifest universe | 88 |
| Unique candidates after in-run dedupe | 86 |
| DUPLICATE_OR_MERGED (in-run) | 2 |
| WOULD_CREATE | 1 |
| WOULD_UPDATE | 85 |
| UNCHANGED | 0 |
| FAILED | 0 |
| Stage status | **DRY_RUN** |
| Persistence certified | **false** (correct for dry-run) |

Founder operation classes: UPDATE_MATERIAL 69 · DISQUALIFIED_OR_HISTORICAL 12 · DUPLICATE_OR_MERGED 2 · RESEARCH_CONTINUATION 3.

Full row plan: `03-BETHESDA_88_DRY_RUN_WRITE_MANIFEST.json`

## 6–7. Contact / evidence / duplicate / idempotency (Level 2 isolated)

Isolated in-memory store (no production Airtable mutation):

- CREATE → read-back match → **PERSISTED_AND_VERIFIED**
- UNCHANGED on rerun
- UPDATE on material title change
- Duplicate composite key collapsed (2→1)
- Empty contact overwrite refused — email preserved

Evidence: `05-LEVEL2_ISOLATED_WRITE_READBACK.json`

## 8. Isolated write/read-back

Authorized as **mock/isolated only**. Production Bethesda rows not modified.

## 9. Two-run repeatability

Two identical dry-run plans over the same 88-universe input: **fingerprints identical**.

Evidence: `04-TWO_RUN_REPEATABILITY.json`

## 10. FQO / commercial qualification regression

No FQO contract changes in this PR beyond certificate stage-status reporting. Hotel-wide / FQO suites unchanged in behavior; STAGED quality 68 and CURRENT gold 67 protected.

## 11. STAGED / CURRENT integrity

| Pointer | Version | Status |
|---|---|---|
| STAGED | quality_v1 **68** | protected |
| CURRENT | local_master_v2 **67** | protected |

Evidence: `07-STAGED_CURRENT_INTEGRITY.json`

## 12. Wyndham / Waterstone cross-hotel safety

Dry-run plans from local `opportunities.json` (capped):

- Wyndham San Diego Bayside `recnPB3Q02NO8Ma1o` — foreign hotel rows: **0**
- Waterstone Boca Raton `recgMYovrrZDJMqzX` — foreign hotel rows: **0**

Evidence: `06-CROSS_HOTEL_SAFETY.json`

## 13. Production write permissions / gates

All required:

1. `authorizeResearchPersistence=true`
2. Non-empty `exactWriteSetAuthorization` founder token
3. `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES=1`
4. `apply=true`
5. Prefer `readBack=true`
6. `authorizePublish=false`
7. `authorizeGdoPersistence=false`

Paid research authorization alone **does not** unlock Airtable persistence.

## 14. Exact founder approval request (Level 3 — NOT run)

See `09-FOUNDER_PRODUCTION_WRITE_APPROVAL_REQUEST.json`:

- Destination: GDI Hotel Research Opportunities only
- Universe: 88 research identities (86 unique upserts after dedupe)
- Planned creates: **1** · updates: **85** · suggested write budget: **86**
- No GDO / STAGED / CURRENT side effects

**Approve this manifest before any bulk Bethesda production Airtable write.**

## 15. Implementation / tests

Code:

- Stage status vocabulary + certificate fields on bridge / canonical entry
- Certification runner + permanent stage-status test
- npm: `test:gdi-full-e2e-airtable-persistence-stage-status-v1`, `gdi:full-e2e-airtable-persistence-certification-v1`

Tests run:

- `npm run test:gdi-full-e2e-airtable-persistence-stage-status-v1` → PASS
- `npm run test:gdi-full-e2e-research-persistence-bridge-v1` → PASS
- `npm run gdi:full-e2e-airtable-persistence-certification-v1` → PASS

### Completion classification (Airtable persistence gate)

- Local/mock proof: **PERSISTENCE_VALIDATED** (isolated store)
- Bethesda production Airtable: **PERSISTENCE_DESIGNED** + Level-1 dry-run complete — awaiting founder Level-3 authorization
- Not claimed: `FULL_E2E_VALIDATED` / `PERSISTENCE_CERTIFIED` for live Bethesda writes

**STOP — no bulk production Airtable writes.**

---

## Amendment — P0 pre-write integrity gate (PR #599)

Founder accepted Level 1 dry-run + Level 2 mock; **rejected** immediate 86 production writes pending integrity gate.

Follow-on evidence: `reports/gdi-airtable-persistence-prewrite-integrity-gate-v1/FOUNDER_INTEGRITY_GATE_REPORT.md`

| Gate | Result |
|---|---|
| `commercialEligibilityStatus=GLOBAL_ORG_ONLY` | Correct write-routing; hotel FQO in `researchPayloadJson.hotelSpecificQualification` |
| Lossless merge vs live Airtable | Contacts / evidence / org preserved; no wipe of live research |
| `lastMaterialChangeAt` | No longer material-diff driver |
| Round-trip | 0 failures |
| Canary | Prepared only — `gdi_e2d_770e3b44ad97c1ae` CREATE (awaiting founder GO) |

**Integrity verdict: READY_FOR_CANARY** (not production-certified).

---

## Amendment 2 — Validated-68 write scope (founder policy)

Production write source revised to **certified STAGED 68 only** (`…_quality_v1`). Broader 88 / prior 86 set **not approved**. Prior canary `gdi_e2d_770e3b44ad97c1ae` **rejected** (research continuation).

New evidence: `reports/gdi-airtable-persistence-validated-68-write-manifest-v1/FOUNDER_VALIDATED_68_WRITE_SCOPE_REPORT.md`

| Item | Result |
|---|---|
| Write-eligible | 68 FQO |
| WOULD_CREATE / UPDATE / UNCHANGED | 0 / 68 / 0 |
| Nonqualified mutations | 0 (preserve in place) |
| Replacement canary | `gdi_s4912_512deebd0963` (Liz Conrow / existing `recxVyeNwB23XsC5G`) |
| Idempotent mock rerun | PASS (run3 UNCHANGED × 68) |

**Scope verdict: READY_FOR_VALIDATED_CANARY** — awaiting founder GO; no live writes.
