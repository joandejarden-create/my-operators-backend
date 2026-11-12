# GDI Commercial Intelligence Consistency Audit & Remediation

**Contract:** `gdi-commercial-intelligence-consistency-audit-v1`  
**PR:** #222  
**Paid research:** **$0**

---

## Final gate (post-correction)

| Metric | Result |
|---|---|
| Critical contradictions (after) | **0** |
| High contradictions (after) | **0** |
| Verified CMI | **4** |
| Demoted CMI | **3** |
| Baseline IDs | **67** preserved |
| Published | **19** unchanged |
| CSV columns | **64** |

---

## NADO 2028 revalidation

**Finding:** Crystal Gateway Marriott **is** the confirmed 2028 host.

| Distinction | Status |
|---|---|
| Confirmed 2028 competitor booking | **YES** — [nado.org/2028washcon](https://www.nado.org/2028washcon/) names Crystal Gateway Marriott with published room rates (weekend $215 / weekday $300 + tax), March 19–22, 2028 |
| Announced 2028 venue | Confirmed (same page) |
| Historical venue pattern | Insufficient alone — **not** used as proof |
| Predicted future location | N/A |

Ledger upgraded from pattern/`ANNOUNCED_VENUE` to **`CONFIRMED_CONTRACTED_BLOCK`** with edition-specific `2028washcon` evidence. Target-hotel sourcing remains **NOT_ESTABLISHED** (no Bethesda Marriott RFP).

---

## Classification loophole closed

`classifyCompetitiveMarketIntelligence` no longer treats a legacy booking flag + generic `sources[]` as verified CMI.

Non-ledger verification now requires **edition-specific** competitor venue/booking evidence (year-tied URL and/or claim language). Historical information is retained for audit; fixtures/seeds alone do not verify.

---

## Seven-record CMI before/after

| ID | After | Rationale |
|---|---|---|
| `gdi_opp_acvnu_renal_week_20270412` | **Verified** | Official Bethesdan contracted block; Marriott sourcing not established |
| `gdi_opp_nado_ddaa_washcon_2027_overflow` | **Verified** | Crystal Gateway 2027 host evidenced; overflow unconfirmed |
| `gdi_opp_nado_ddaa_washcon_2028` | **Verified** | Edition-specific 2028WashCon confirmation |
| `gdi_opp_ecs_251_2027_overflow` | **Verified** | Marriott Marquis + WEWCC |
| `gdi_opp_afcea_hits_2027` | **Demoted** | 2026 Bethesda North; Hyatt unsupported |
| `gdi_opp_world_biomaterials_2028_overflow` | **Demoted** | WEWCC only |
| `gdi_opp_aad_2028_overflow` | **Demoted** | No hotel block |

---

## Presentation certification (Bethesda)

Authorized sales inventory (`:8092` PR branch):

- Sales Opportunities **67**; CMI filter **4**; All Commercial Intelligence **67** unique
- ACVNU: competitor **CONFIRMED_CONTRACTED_BLOCK** (The Bethesdan); sourcing **NOT_ESTABLISHED**; action qualifies overflow — **do not request an RFP as host**
- AFCEA demoted: no verified CMI; competitor corrected to Bethesda North
- Published audience **19**
- CSV **64** columns; remediation language present

---

## Detector improvements (false positives documented)

Not suppressed — rules tightened:

1. Status enum/label strings (`OVERFLOW_POSSIBLE`, “Overflow Possible”) no longer counted as narrative claims.
2. Proper overflow disclaimers (“overflow … unconfirmed”) exempt from HIGH overflow claims.
3. Affirmative open-path language still flags when present without disclaimer.
4. Presentation sanitizer rewrites unsafe open-RFP / “overflow opportunity” copy when sourcing is unconfirmed.

---

## Tests

- `npm run test:gdi-commercial-intelligence-consistency-audit-v1`
- `npm run test:gdi-bethesda-67-sales-baseline-cmi-v1`
- `npm run test:gdi-bethesda-67-sales-team-inventory-display-v1`
- `npm run test:gdi-bethesda-67-inventory-access-control-v1`
- `npm run gdi:commercial-intelligence-consistency-audit`

Preserves hotel-scoped access controls from PR #220/#223, HCP V3, no auto-publication, zero paid research.
