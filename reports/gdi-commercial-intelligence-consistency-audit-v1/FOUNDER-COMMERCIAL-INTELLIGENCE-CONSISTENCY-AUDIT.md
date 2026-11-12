# GDI Commercial Intelligence Consistency Audit & Remediation

**Contract:** `gdi-commercial-intelligence-consistency-audit-v1`  
**Base:** main `@ b724692b9` (includes PRs #217–#220)  
**Paid research:** **$0**

---

## Root causes

1. **`Boolean(seed)` CMI verification** — presence in `BETHESDA_EXA_VERIFIED_CMI_SEEDS` counted as booking proof.
2. **Overflow thesis path** — `overflow` + named competitor + `PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE` auto-verified CMI.
3. **Collapsed dimensions** — competitor venue confirmation was conflated with target-hotel open sourcing and RFP actions.
4. **Wrong competitor attribution** — AFCEA Hyatt vs Bethesda North; ECS Bethesda North vs Marriott Marquis; Biomaterials/AAD hotel blocks without sources.
5. **Stale narrative** — ACVNU still said “lodging demand path open” / “request RFP” while official page shows a contracted Bethesdan block only.

---

## ACVNU Renal Week (canonical correction)

| Dimension | After |
|---|---|
| Competitor venue | **CONFIRMED_CONTRACTED_BLOCK** — The Bethesdan (Apr 11–17, 2027) |
| Target sourcing | **NOT_ESTABLISHED** (no Marriott RFP/overflow on official page) |
| Attendance | **250+** (official site) |
| Actionability | **Competitive Market Intelligence** |
| Next step | Qualify overflow with housing decision-maker — **do not request RFP as host** |
| Contact | JD Foster = activity-connected chair — **not** verified booking buyer |

Sources: `https://acvnurenalweek.org/Plan-Your-Trip/Hotel-Accommodations`, `https://acvnurenalweek.org/`

---

## Seven-record CMI before/after

| ID | Before | After | Rationale |
|---|---|---|---|
| `gdi_opp_acvnu_renal_week_20270412` | Verified (seed) | **Verified** | Official contracted block + attendance; sourcing corrected |
| `gdi_opp_nado_ddaa_washcon_2027_overflow` | Verified | **Verified** | Crystal Gateway host evidenced; overflow unconfirmed |
| `gdi_opp_nado_ddaa_washcon_2028` | Verified | **Verified** | Crystal Gateway pattern; monitor 2028 (not Bethesda North) |
| `gdi_opp_ecs_251_2027_overflow` | Verified (wrong hotel) | **Verified** | Corrected to **Marriott Marquis** + WEWCC |
| `gdi_opp_afcea_hits_2027` | Verified (Hyatt) | **Demoted** | 2026 lodging = Bethesda North; 2027 page unavailable; Hyatt unsupported |
| `gdi_opp_world_biomaterials_2028_overflow` | Verified | **Demoted** | WEWCC only — no hotel block |
| `gdi_opp_aad_2028_overflow` | Verified | **Demoted** | No Bethesda North block evidence |

**Verified CMI after remediation: 4** (all on original 67). **Demoted: 3.**

---

## 67-ID reconciliation

- All seven CMI candidates were already `on67Baseline: true`.
- CMI is a **secondary population** on the **same unique IDs** — not 74 cards.
- Dual-classified unique IDs after fix: **4** (Sales Opportunities ∩ verified CMI).
- Original **67 IDs preserved**; published **19** unchanged; no auto-publish.

---

## Contradiction detector

Codes implemented (severity-ranked): open RFP without sourcing, overflow without evidence, competitor mismatch, unsupported CMI verification, attendance missing when sourced, contact remit overclaim, narrative vs source, duplicate cycle, etc.

Bethesda commercial surface (audit run):

| | Before | After remediation overlay |
|---|---|---|
| Rows with findings | 17 | 12 |
| Critical | 5 | 2 |
| High | 12 | 10 |

Remaining findings are mostly other non-ledger commercial rows staged for bounded follow-up (no paid spend).

---

## Presentation / CSV

Remediation applied in `annotateInventoryPopulations` → list cards, drawer DTOs, and CSV share the same normalized fields (`competitorVenueStatus`, `targetHotelSourcingStatus`, corrected actions/attendance).  
PR #217 **64-column** CSV headers preserved.

---

## Unverified candidates

~27 commercial-surface rows retain competitor/overflow signals without ledger verification — research continuation / further verification only.

---

## Cross-hotel

Ledger remediation is Bethesda-scoped. Registry has 19 hotels; framework (`commercial-contradiction-detector` + status dimensions) is reusable without copying Bethesda rules.

---

## Tests

- `npm run test:gdi-commercial-intelligence-consistency-audit-v1`
- `npm run test:gdi-bethesda-67-sales-baseline-cmi-v1`
- `npm run gdi:commercial-intelligence-consistency-audit`

---

## Research recommendations (no spend)

1. Re-fetch AFCEA 2027 HITS housing when organizer page is healthy.  
2. Confirm AAD 2028 official housing list.  
3. Confirm World Biomaterials official hotel block when published.  
4. NADO 2028: confirm named host when announced.

---

## PR / merge

Branch: `feature/gdi-commercial-intelligence-consistency-audit-v1`  
Does **not** widen customer inventory or auto-publish.
