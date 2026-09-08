# HOTEL_INTELLIGENCE_LEARNING_EXTRACTION_AUDIT_V1

> Packet 2.8 · Condensed from forensic audit of frozen corpus · **No Webhound · No new spend**  
> Generated: 2026-09-08  
> Corpus: `HOTEL_INTELLIGENCE_LEARNING_CORPUS_V1` · Registry: `RESEARCH_LEARNING_REGISTRY.json` (5 **CANDIDATE** records)

## Snapshot

| Metric | Value |
|--------|------:|
| Hotels | 4 |
| Real Webhound sessions | **5** |
| Recorded spend | **~$25.4** |
| Native reproduction (critical facts) | **~30% YES / ~45% PARTIAL / ~25% NO** |
| Learning promotion | **None** — all CANDIDATE; `auto_promote` forbidden |
| `finding_id` binding | **COMPLETE** (post-migration): 8/8, 8/8, 6/6, 6/6, 8/8 |

---

## A. Per-run learning extraction (all 5 runs)

> Every learning status: **CANDIDATE** only.

### Run 1 — KGPV Full HI

| Field | Value |
|--------|--------|
| Session | `23969ba8-60d2-402d-887d-c94f555d3e3b` |
| Request / cost / dossier | `req_kgpv_full_hi_run1` · **$5.4** · `dossier_kgpv_full_hi_v1` |
| Hotel | Krystal Grand Puerto Vallarta (`recUNycnMwOVFX0hc`) |
| Archetype | `MX_LISTED_PUBCO_OWNER_OPERATOR` |
| Registry id | `learn_kgpv_full_hi_adjacent_asset` |

| Field | Evidence-backed answer |
|--------|------------------------|
| **WHAT WAS HARD** | Adjacent-name collision (Krystal Grand vs Krystal Resort); Census Breathless vs current Krystal Grand; PropCo IHVSF ≠ deed; natural-person UBO unresolved |
| **SOURCE THAT RESOLVED IT** | GSF Reporte Anual 2025; GSF/hotel first-party; BMV HOTEL; Hyatt Breathless announcement (ANNOUNCED); 2016 event for third-party Krystal PV contrast |
| **QUERY / METHOD** | MX-PUBCO-01/02/03 + HOTEL-IDENTITY-01/02; annual-report / IR probes; adjacent-asset disambiguation (`kgpv-method-reconstruction.json`) |
| **FAILED METHODS** | Management-as-ownership; merging adjacent Resort; promoting Breathless to CURRENT |
| **NEGATIVE SCREENS** | `ADJACENT_ASSET`, `WRONG_PROPERTY`, `ANNOUNCED_NOT_CURRENT`, `OPERATOR_NOT_OWNER` (+ related) |
| **STRUCTURED OUTPUT** | Deep fixture + dossier + module archive; claim_handoff CANDIDATE |
| **NATIVE REPRO?** | **PARTIAL** |
| **PLAYBOOK CHANGE** | Graduate adjacent-asset guard; PropCo candidate ≠ deed |
| **EVAL** | Blind native vs KGPV benchmark A01–A08; zero Breathless/Resort merges |
| **finding_id** | **8/8** (migration complete) |

---

### Run 2 — KGPV Change & Opportunity

| Field | Value |
|--------|--------|
| Session | `9839ce88-4b31-4f41-baac-208921f7a06e` |
| Run / cost / report | `run_681a137646388167` · **$5** · `addendum_change_opportunity_ff3e5bf2` |
| Template | `CHANGE_OPPORTUNITY` 1.1.0 |
| Archetype | `BRAND_CONVERSION_SLIP` |
| Registry id | `learn_kgpv_co_conversion_slip` |

| Field | Evidence-backed answer |
|--------|------------------------|
| **WHAT WAS HARD** | Announced Breathless conversion incomplete as of Sep 2026; collaboration ≠ franchise/mgmt |
| **SOURCE THAT RESOLVED IT** | Hyatt newsroom 2024-02-14; Open Jaw Apr-2025 target; Inclusive Collection inventory absence; renovation signals (`source_count: 34`) |
| **QUERY / METHOD** | Opening-target verification; brand-inventory negative check; agreement-structure parse |
| **FAILED METHODS** | Treat 2025 target as completed; collaboration as finished franchise |
| **NEGATIVE SCREENS** | `ANNOUNCED_NOT_CURRENT`, `GUEST_PATTERN_NOT_STRUCTURAL_PROOF` |
| **STRUCTURED OUTPUT** | Addendum with 8 keyed findings |
| **NATIVE REPRO?** | **PARTIAL** |
| **PLAYBOOK CHANGE** | Announced-conversion-slip lane for CHANGE_OPPORTUNITY |
| **EVAL** | Breathless remains ANNOUNCED when first-party still Krystal Grand |
| **finding_id** | **8/8** |

---

### Run 3 — Cambridge Beaches Full HI

| Field | Value |
|--------|--------|
| Session | `9d6b0a8d-e038-44ce-ab61-2ee92a2a4807` |
| Request / cost / dossier | `req_cambridge_full_hi_run1` · **$5** · `dossier_cambridge_beaches_full_hi_v3` |
| Hotel | Cambridge Beaches (`recIwaP1etgx2g9nA`) |
| Archetype | `PRIVATE_CROSS_BORDER_OPAQUE` |
| Registry id | `learn_cambridge_private_opaque` |

| Field | Evidence-backed answer |
|--------|------------------------|
| **WHAT WAS HARD** | Offshore PropCo via Tourism Order; contested operator residual listings; seller identity discrepancy; acreage conflict |
| **SOURCE THAT RESOLVED IT** | Tourism Investment Order 2022 + ministerial statement; Dovetail first-party; Royal Gazette / Bernews; Butterfield; Benchmark/Pyramid residual |
| **QUERY / METHOD** | `PRIVATE_OPAQUE_OWNER_PLAYBOOK`, `GOVERNMENT_DEVELOPMENT_ORDER_LANE`, `TEMPORAL_OPERATOR_RESOLUTION_LANE`, seller-discrepancy / profile lanes |
| **FAILED METHODS** | Benchmark 2021 as current operator; Order as physical completion; title as UBO |
| **NEGATIVE SCREENS** | `SPONSOR_NOT_DEED_UBO`, `RESIDUAL_PORTFOLIO_NOT_CURRENT_OPERATOR_PROOF`, `ORDER_APPROVAL_IS_NOT_PHYSICAL_COMPLETION` |
| **STRUCTURED OUTPUT** | Deep + dossier v3; `webhound_source_count: 35` |
| **NATIVE REPRO?** | **PARTIAL** |
| **PLAYBOOK CHANGE** | PRIVATE_OPAQUE + GOVERNMENT_DEVELOPMENT_ORDER playbooks |
| **EVAL** | Operator temporal CURRENT vs residual listing |
| **finding_id** | **8/8** |

---

### Run 4 — Sheraton Guadalajara Expo Full HI

| Field | Value |
|--------|--------|
| Session | `e4a61251-a016-4c47-a5e4-a1ff57b996cb` |
| Run / cost / dossier | `run_sheraton_gdl_full_hi_run1` · **$5** · `dossier_sheraton_gdl_expo_full_hi_v1` |
| Hotel | Sheraton Guadalajara Expo (`recsYJb2R1jarPpK3`) |
| Archetype | `MX_PRIVATE_PROPCO_FRANCHISE` |
| Registry id | `learn_sheraton_gdl_profeco_propco` |

| Field | Evidence-backed answer |
|--------|------------------------|
| **WHAT WAS HARD** | Private PropCo without deed; HOTSSON→Sheraton chronology; Aimbridge vs owner; Newton UBO PROBABLE |
| **SOURCE THAT RESOLVED IT** | Marriott first-party; Aimbridge portfolio PDF; Hospitality Net; **PROFECO RPCA** (RFC IHN120620UZ3) |
| **QUERY / METHOD** | FULL_HI prompt; Mexico private-opaque families; PROFECO adhesion as PropCo resolver |
| **FAILED METHODS** | PROFECO equals deed; Aimbridge as owner; sibling hotel merge |
| **NEGATIVE SCREENS** | `OPERATOR_NOT_OWNER`, `TITLE_NOT_AUTHORITY`, `SOURCE_NOT_DECISIVE` |
| **STRUCTURED OUTPUT** | Deep + Mexico adapter dossier; claim_handoff CANDIDATE |
| **NATIVE REPRO?** | **PARTIAL** (PROFECO path not READY-named playbook) |
| **PLAYBOOK CHANGE** | MX-PRIVATE-PROPCO via consumer-protection/adhesion contracts |
| **EVAL** | PropCo HIGH requires RFC/incorporation or deed |
| **finding_id** | **6/6** |

---

### Run 5 — voco / Real Inn Cancún Full HI

| Field | Value |
|--------|--------|
| Session | `799762c4-5a98-488c-890c-d8e5acc8c616` |
| Run / cost / dossier | `run_real_inn_cancun_full_hi_run1` · **$5** · `dossier_real_inn_cancun_full_hi_v1` |
| Hotel | voco Cancún Zona Hotelera (`recTYaiA4S6fR6ixx`) |
| Archetype | `SAME_ASSET_REFLAG_PACKAGE_OWNER` |
| Registry id | `learn_voco_reflag_package_owner` |

| Field | Evidence-backed answer |
|--------|------------------------|
| **WHAT WAS HARD** | Same-asset reflag; PropCo UNKNOWN; Alliance package owner without Mexican vehicle; Justo legal bridge unknown |
| **SOURCE THAT RESOLVED IT** | IHG package disclosure; trade press Alliance owner; Aimbridge operator separation |
| **QUERY / METHOD** | Same-asset identity rule; package-level ownership; Aimbridge as operator |
| **FAILED METHODS** | Invent PropCo; census Real Inn as CURRENT; Justo as deed UBO |
| **NEGATIVE SCREENS** | `ANNOUNCED_NOT_CURRENT`, `OPERATOR_NOT_OWNER`, `INSUFFICIENT_IDENTITY_MATCH` |
| **STRUCTURED OUTPUT** | Deep + Mexico adapter; six-hotel portfolio notes |
| **NATIVE REPRO?** | **PARTIAL** for brand/operator package; **NO** for PropCo/Justo↔Alliance bridge |
| **PLAYBOOK CHANGE** | Same-asset reflag + portfolio-package-owner confidence class ≠ PropCo |
| **EVAL** | Fail if PropCo invented or census overrides CURRENT voco |
| **finding_id** | **6/6** |

---

## B. Native reproduction summary

| Status | Share | Notes |
|--------|------:|-------|
| YES | ~30% | Encoded query→source→claim paths (esp. KGPV MX-PUBCO identity / ownership basics) |
| PARTIAL | ~45% | Public-web verifiable but not blind-proven / playbook not READY |
| NO | ~25% | Deeds, natural-person UBOs, Justo↔Alliance legal bridge, loan amounts, member registers |

No green `parity-evaluation.json` / completed blind-native dossier outputs under packet reports.

---

## C. Metrics per run (extractable)

| Run | Cost | Sources | Findings | Open Qs | People (deep) | Relationships |
|-----|-----:|--------:|---------:|--------:|--------------:|--------------:|
| KGPV Full HI | $5.4 | 20 dossier / 11 curated | 8 | 6 | 10 | 12 |
| KGPV C&O | $5 | 34 | 8 | 9 | — | — |
| Cambridge Full HI | $5 | 35 | 8 (dossier v3) | 8 | 5 | 7 |
| Sheraton Full HI | $5 | 15 dossier | 6 | 11 | 6 | 8 |
| voco Full HI | $5 | 15 dossier | 6 | 13 | 9 | 6 |

---

## D. Common primitives vs archetypes (condensed)

**COMMON:** identity resolution; temporal brand statuses; operator≠owner; announced≠current; title≠authority; PropCo≠deed; first-party waterfall; VERIFIED-only profile links; claim handoff CANDIDATE; shared Full HI chapter skeleton; rooms helpers; negative-screen vocabulary.

**ARCHETYPE-SPECIFIC (CANDIDATE):** MX listed pubco OO (KGPV); private cross-border + tourism order (Cambridge); MX private PropCo via adhesion (Sheraton); same-asset reflag + package owner (voco); conversion-slip C&O (KGPV addendum).

---

## E. Hard stops

- Learnings remain **CANDIDATE** — do not auto-mutate production prompts  
- Four hotels remain permanent regression fixtures — not the 25-hotel pilot set  
- Packet Webhound runs: **0**
