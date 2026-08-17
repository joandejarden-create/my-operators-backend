# VIC → Brand Explorer Sandbox Lane Closure

**Status:** `vic_be_sandbox_lane_paused_ready_for_operator_explorer`  
**Decision:** Pause VIC → Brand Explorer sandbox expansion. Do **not** start production patch. Return to **Operator Explorer** priorities.

---

## 1. Executive summary

The Mexico VIC → Brand Explorer lane proved a safe sandbox path from locked VIC property evidence into owner-facing Brand Explorer Presentation copy (property examples, Mexico/CALA footprint, portfolio context, property proof). Small (16) and medium (28) pilots passed founder review with zero production writes and an intact protected Active **62** baseline.

This lane is now **paused**. Keep the sandbox base for future staging. Resume only with explicit instructions. Next recommended priority: **Operator Explorer**.

---

## 2. Current approved stopping point

| Item | Value |
|------|-------|
| Founder decision | `medium_sandbox_founder_review_approved_pause_vic_lane` |
| Closure status | `vic_be_sandbox_lane_paused_ready_for_operator_explorer` |
| Production patch | **Blocked** unless separately requested |
| Sandbox mutation at closure | **None** (docs only) |

---

## 3. Frozen 62 baseline status

| Check | Result |
|-------|--------|
| Decision | `frozen_62_active_public_full_baseline_semantic_clean_flex_held` |
| Active universe | **62** |
| Semantic C/H/M | **0/0/0** |
| Four Points Flex | Held (Under Review) — outside Active/Live public-full |
| Mutated by VIC lane | **No** |

Artifacts:

- `reports/brand-explorer-62-active-public-full-baseline.json`
- `docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md`
- `lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js`

---

## 4. VIC 4-family baseline status

| Check | Result |
|-------|--------|
| Status | `mexico_vic_4family_baseline_locked_staging_ready` |
| Freeze hash | `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3` |
| Path | `data/research-engine-v2/verified-independent-census-mexico-combined-4family/` |
| Mutated by VIC→BE pilots | **No** (read-only authority for lineage) |

---

## 5. Sandbox Airtable status

| Item | Value |
|------|-------|
| Base | **Deal Capture MVP — Sandbox** (`appRbW…2ch1`) |
| Keep for future staging | **Yes** |
| Production base | `appvtn…INP6` (read-only comparison only) |
| Env | `AIRTABLE_ENV=sandbox`, `AIRTABLE_BASE_ID_SANDBOX`, `BE_PILOT_SANDBOX_CONFIRMED=1` |
| Token note | Prefer `AIRTABLE_PAT` / `AIRTABLE_SANDBOX_API_KEY` if `AIRTABLE_API_KEY` is production-only |
| Validate | `npm run research-engine-v2:validate-airtable-sandbox` |

Sandbox writes (historical, during pilots only): Presentation **Title / Body / Slot Key / Brand**. No Brand Status, release, CV, Brand Verified, or Recent Momentum writes.

---

## 6. Small pilot summary

| Item | Value |
|------|-------|
| Slot keys | `vic.pilot.property_examples`, `…geographic_footprint_mexico`, `…portfolio_context`, `…owner_facing_copy` |
| Rows | **16** |
| Brands | hotel-indigo, ascend, curio-collection, holiday-inn-express |
| Properties | **10** |
| Review | `vic_be_small_pilot_sandbox_review_approved_continue_expanded_sandbox` |
| Preserved after medium | **Yes** (16/16 exact small slots still present) |

---

## 7. Medium pilot summary

| Item | Value |
|------|-------|
| Slot prefix | `vic.pilot.medium.*` |
| Rows | **28** (7 brands × 4 slots) |
| Properties | **25** |
| Brands | hotel-indigo, ascend, curio-collection, holiday-inn-express, voco-hotels, kimpton, avid-hotels |
| Copy rejects | **0** |
| Rulings | **PASS** |
| Founder review | `medium_sandbox_founder_review_approved_pause_vic_lane` |
| Overwrote small pilot | **No** |

---

## 8. What was proven

1. Dedicated sandbox isolation + validation gate before writes.
2. VIC freeze-hash lineage into owner-facing Presentation copy.
3. Safe content types: **property examples**, **Mexico/CALA footprint**, **portfolio context**, **property proof**.
4. Steward rulings: El Cid soft-brand only; Amberes proof/example; MS Milenium = San Pedro Garza García / Monterrey metro.
5. Parallel slot namespaces (`vic.pilot.*` vs `vic.pilot.medium.*`) without collision.
6. No automatic Recent Momentum / rooms / owner / operator / open dates / affiliation start / CV / Brand Verified / Brand Status from VIC.
7. Production Active **62** and semantic **0/0/0** remained clean after sandbox pilots.

---

## 9. What remains unproven

1. **Production** Brand Explorer Presentation apply of VIC-derived copy.
2. Multi-country / full-catalog VIC → BE automation beyond Mexico 4-family sandbox.
3. Steward-free slug mapping for every large-tier candidate brand.
4. Image / gallery enrichment from VIC.
5. Any path that creates Recent Momentum or fabricated rooms/owner/operator/dates from VIC (intentionally out of scope).

---

## 10. Production patch status

**Do not run a production patch unless separately requested.**

This closure does **not** authorize production writes. Sandbox results are staging evidence only.

---

## 11. Resume instructions

When resuming VIC → BE later:

1. Re-read this closure packet + `mexico-vic-be-medium-sandbox-founder-review.*`.
2. Confirm frozen 62 still clean (SoT / semantic / mandatory gates as needed).
3. Confirm VIC freeze hash still `c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3` unless a new VIC freeze is explicitly approved.
4. Run `npm run research-engine-v2:validate-airtable-sandbox` → must be `airtable_sandbox_validated_ready_for_vic_be_patch`.
5. Do not overwrite existing `vic.pilot.*` / `vic.pilot.medium.*` without explicit `--replace` / replace mode.
6. Production apply requires a **new explicit task** with production safety gates — not implied by this pause.

Useful commands:

```bash
npm run research-engine-v2:validate-airtable-sandbox
npm run research-engine-v2:mexico-vic-be-medium-sandbox-founder-review
npm run research-engine-v2:mexico-vic-be-expanded-sandbox-pilot -- --dry-run
```

---

## 12. Risks and guardrails

| Risk | Guardrail |
|------|-----------|
| Accidental production write | Sandbox validation; never target `AIRTABLE_BASE_ID` for VIC pilots |
| Overwriting small with medium | Separate slot namespaces; no replace without flag |
| False Recent Momentum | Forbidden — VIC property existence ≠ momentum |
| Ownership / management claims | Steward rulings (esp. Ascend / El Cid / Faranda / Choice) |
| Fabricated rooms / dates / operators | Forbidden auto fields |
| Stale freeze | Always cite VIC freeze hash + frozen 62 decision |
| Token confusion | Use PAT/sandbox key that can see sandbox base |

**VIC may support:** property examples, Mexico/CALA grounding, portfolio context, property proof.

**VIC must not automatically create:** Recent Momentum, owner, operator, rooms, opening dates, affiliation start dates, Company Validated, or Brand Verified.

---

## 13. Recommended next priority: Operator Explorer

**Next lane:** Operator Explorer.

- Checklist: `docs/data-intelligence/operator-explorer-ready-for-next-operator.md`
- Project memory: `AGENTS.md` (Operator Explorer quality baseline + Tab Factory)
- Typical gates:

```bash
npm run test:operator-explorer-os
npm run operator-explorer-os -- --source=merged --dry-run
```

When `canStartNextOperatorExplorer=true`, follow the factory queue (e.g. GHL Hoteles / batch docs) — not VIC production promotion.

---

## Explicit statements

1. **Do not run production patch unless separately requested.**
2. **Keep sandbox base** (Deal Capture MVP — Sandbox) for future staging.
3. **VIC can support** property examples, Mexico/CALA grounding, portfolio context, and property proof.
4. **VIC must not automatically create** Recent Momentum, owner/operator, rooms, opening dates, affiliation start dates, Company Validated, or Brand Verified.
5. **Next recommended lane:** Operator Explorer.
