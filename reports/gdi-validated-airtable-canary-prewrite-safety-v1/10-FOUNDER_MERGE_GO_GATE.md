# PR #599 Final Merge & Canary Release Gate

**As of:** 2026-11-25  
**PR tip:** `421d038da64bf066b363d5378ea7db9892ed6105`  
**Canary executed:** **NO**  
**Remaining 67 updates:** **NOT EXECUTED**

---

## Verdict

### `NOT_READY_FOR_FOUNDER_GO`

CI on tip is **green**. Single-record authorization envelope is **correct**. Pre-write snapshot + rollback plan are **valid**.

Historical intelligence preservation for the proposed Airtable `researchPayloadJson` write is **not** fully demonstrated. Writing the proposed slim payload would replace the live Airtable SoT fat ORS with a degraded representation that also **changes commercial meaning**.

**Do not merge for canary unlock. Do not authorize the canary until persistence representation is corrected.**

---

## 1. CI (latest tip `421d038`)

| Check | Result |
|---|---|
| GDI recovery regression suite | **pass** |
| Admin tabs + AVA panel | **pass** |
| Radar HE + ADP free gates | **pass** |
| Spice fixture + prod isolation | **pass** |
| gitleaks | **pass** |
| Mergeable | **MERGEABLE** |
| State | **OPEN** — not merged |

---

## 2. Semantic comparison: 42,314 → 7,234 `researchPayloadJson`

| Metric | Value |
|---|---|
| Original chars / SHA-256 | **42314** / `4cf0ff7074bdbcae767574b062690306c822033be9cf5265d325843e9ee24d12` |
| Proposed chars / SHA-256 | **7234** / `5084651e6f0eb9c379a8cd66bdb14ab2cbe4829d34f87299ff40fef61a2cd2ba` |
| Top-level keys omitted | **~190** |
| Nested key paths omitted | **519** |
| Contacts named preserved | **Yes** (4/4 names) |
| Evidence HTTP URLs | 9 structured → 5 URL stubs (URLs retained; field values/confidence lost) |

### Critical semantic regressions in proposed write

| Field | Live (Airtable SoT) | Proposed slim | Risk |
|---|---|---|---|
| `revenueStreamClass` | `COMBINED_GUESTROOMS_EVENT_FNB` | `GUESTROOMS_ONLY` | **Commercial meaning corruption** |
| `commercialDemandType` | `BOTH` | *(omitted)* | Demand-type intelligence dropped |
| `whatIsHappening` | Accurate conference summary (`summaryWhat`) | “exhibitor / vendor programme contact …” | **False commercial narrative** |
| `meetingHistory` | Present (2025/2026 venues) | Omitted | Historical cycle evidence dropped from SoT |
| `competitors` / STR set | Present | Omitted | Competitive intelligence dropped from SoT |
| Evidence objects | 9 field-level FACT rows w/ confidence | 5 URL-only stubs | Provenance detail dropped from SoT |
| Contact grades / audit / whyThisContact | Full Grade A audit | Slim channel fields only | Contact intelligence depth dropped |
| Fit scores / territory / sourcing / venue status | Full ORS | Mostly omitted | Research depth dropped |

Full omit list: `09-SEMANTIC_PAYLOAD_OMIT_ANALYSIS.json`

### Where originals remain durably recoverable **today** (before write)

| Location | Completeness | Notes |
|---|---|---|
| `04b-ORIGINAL_researchPayloadJson.json` | **Byte-identical** fat payload | SHA matches live Airtable snapshot |
| `04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json` | Full typed fields + payload | Rollback source of truth |
| `data/.../recLuxvwwxID7U2B8/opportunities.json` | Local research store | Contains NICE with fat commercial fields |
| Bethesda STAGED quality_v1 / CURRENT gold | Presentation packages | Not a full ORS substitute |
| Checkpoints under `reports/gdi-bethesda-88-*` | Partial | Not byte-identical fat SoT |

**After a slim write to Airtable:** live SoT would no longer hold the fat ORS. Recovery would depend on local snapshot/research files remaining intact — **not** an Airtable-native durable archive. That fails the founder objective: certify Airtable persistence **without sacrificing** proprietary intelligence already collected.

---

## 3. Pre-write snapshot & rollback — still valid

| Item | Status |
|---|---|
| Complete snapshot | `04-COMPLETE_LIVE_PREWRITE_SNAPSHOT.json` |
| Exact payload file | `04b-ORIGINAL_researchPayloadJson.json` |
| Payload SHA verified | `4cf0ff70…ee24d12` **match** |
| Truncated? | **No** |
| Rollback plan | `06-ROLLBACK_PLAN.json` — restore snapshot fields + exact payload bytes |
| Contacts lost in proposed merge? | `lostContacts: []` (names preserved; depth not) |

Rollback remains valid **until** a write occurs. Do not execute write until representation is fixed.

---

## 4. Single-record authorization — confirmed

| Constraint | Value | Verified |
|---|---|---|
| Airtable record | `rec8QDHQEpGbi8kvW` | Yes |
| Opportunity | `gdi_opp_nice_2027` | Yes |
| Policy | `VALIDATED_FQO_ONLY` | Yes (`01-WRITE_GUARD_VERIFICATION.json`) |
| `allowedOpportunityIds` | `[gdi_opp_nice_2027]` | Yes (`03-ALLOWED_ID_RESTRICTION_PROOF.json`) |
| `writeBudget` | `1` | Yes |
| Exact auth token | `FOUNDER_CANARY_gdi_opp_nice_2027` | Documented |
| Env gate | `GDI_RESEARCH_INVENTORY_PRODUCTION_WRITES` currently **off** | Yes |
| GDO / STAGED / CURRENT writes | **None** in canary plan | Yes |
| Remaining 67 | Explicitly not implied | `07-REMAINING_WRITE_MANIFEST_AFTER_CANARY.json` |

Exact field diff (proposed): `05-EXACT_CANARY_FIELD_DIFF.json`  
Changed typed fields: `segment`, `sourceSummary`, `dgGeneratorId`, `researchPayloadJson` (+ `lastMaterialChangeAt` stamp).

---

## 5. Required correction before GO

Persistence must **not** replace fat Airtable ORS with slim-only content that:

1. Demotes `COMBINED_GUESTROOMS_EVENT_FNB` → `GUESTROOMS_ONLY`
2. Injects exhibitor/vendor `whatIsHappening` over verified conference summary
3. Drops `meetingHistory`, competitors, commercial demand type, evidence field values, contact grades/audit

**Acceptable approaches (pick one, then re-diff):**

- **A. Lossless overlay:** keep live fat `researchPayloadJson` keys; overlay only `hotelSpecificQualification` / FQO + additive fields; never re-slim existing fat SoT rows.
- **B. Dual durable write:** archive full original payload to a dedicated durable artifact/field **before** any slim inventory write; prove round-trip recoverability from that archive.
- **C. Slim only for new empty rows;** existing fat rows stay fat (or grow via additive merge).

Until one of these is implemented and re-certified for `gdi_opp_nice_2027`, **withhold merge-for-canary and founder GO**.

---

## 6. Merge decision (Integration Controller)

| Action | Decision |
|---|---|
| Merge PR #599 now for canary unlock | **NO** |
| Execute canary | **NO** |
| Execute remaining 67 | **NO** |
| Status | **`NOT_READY_FOR_FOUNDER_GO`** |

CI green alone does not authorize canary. Persistence representation must be corrected first.
