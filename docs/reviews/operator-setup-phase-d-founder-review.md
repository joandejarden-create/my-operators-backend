# Operator Setup Phase D — Founder Review

**Mode:** apply (2026-08-10)  
**Backup:** `backups/operator-setup/phase-d/2026-08-10T17-30-21`  
**Writes:** 724 / 724 · **Failures:** 0 · **Fit/scoring:** unchanged · **Owner pilot:** disabled

---

## Are the Operator Setup tables now actually populated for the real companies?

**Yes — for retained Production sections — with explicit Partial / N/A reasons, not unexplained blanks.**

Live Airtable previously looked empty because (1) the dense first ~10 rows are **Test Fixtures**, and (2) many Production operators only had Phase C Operating Platform / Brand Relationship scaffolds while Platform, Commercial, Governance, Engagement, Infrastructure, and Leadership stayed thin.

Phase D filled **36 Production** operators with OE-evidence narratives and section rows. Fixture prose was **not** cloned.

| Metric | Before | After |
| ------ | -----: | ----: |
| Complete section cells | 116 | **263** (66.4%) |
| Partial (with reason) | 92 | **86** |
| Empty (unexplained) | 141 | **0** |
| Complete ∪ Partial ∪ N/A | 62.4% | **100%** |
| Writes / failures | — | **724 / 0** |

Aspirational ≥90% **Complete** is not met (66.4%). The binding Phase D bar — **100% Complete / Partial-with-reason / N/A** with **zero unexplained Empty** — **is met**.

---

## 1. Visual sparsity baseline

See `reports/operator-setup-phase-d-live-section-coverage.md`.

Worst live gaps before Phase D: Platform & Markets (~11%), Governance / Engagement / Leadership (~6–11%), Commercial (~28%). Operating Platform + Brand Relationships were already ~97% after Phase C.

## 2. Production section matrix before

`reports/operator-setup-production-section-matrix.md` — 396 cells; 141 Empty.

## 3. Golden / Test Fixture audit

`reports/operator-setup-phase-d-golden-fixture-audit.md`

Dense early rows (Viento Sur, Mangle Azul, Cordillera One, Antillano Norte, Panamerican Lodging, Río Plata, Barrio Hotelero CDMX, Metro Lodging São Paulo, Oro Verde) = **Test Fixtures**. Arbor / Hotel Equities = protected golden baselines (curated, not synthetic). Fixture templates were **not** used as Production truth models.

## 4. Retained section contract

`docs/data/operator-setup-retained-section-contract.md`

## 5. Existing-data reuse

OE Assignments, Market Presence, Brand Relationships, Master OM/MA/website, profile/website packs. No mass external research spend.

## 6. Targeted research performed

**0** new primary-source research packages. Gaps without OE footprint remain Partial with reason (packs still missing for ~16 operators’ deep narratives).

## 7. Webhound use

`reports/operator-setup-phase-d-webhound-use.md` — Track 2 classified (~27 unique-useful); **0 merged** into Setup/Assignments (primary-source validation gate not cleared).

## 8. Writers created/modified

`lib/operator-setup/phase-d-section-writers.js` + `scripts/operator-setup-phase-d-apply.mjs`

- phase-d-oe-profile (+ create)
- phase-d-platform (+ create)
- phase-d-governance (+ create)
- phase-d-commercial (narrow; no bf_*)
- phase-d-leadership-platform
- phase-d-engagement / phase-d-infrastructure

Blocked: bf_*, geo_*, locationType*, *Experience %, cap_kpi_/signal_, infra_kpi_/signal_, invented executives.

## 9. Table-by-table writes

| Batch | Section | Written |
| ----: | ------- | ------: |
| 1 | Profile | 148 |
| 2 | Platform & Markets | 84 |
| 3 | Governance | 60 |
| 4 | Commercial Fit | 91 |
| 5 | Leadership Platform | 151 |
| 6 | Engagement + Infrastructure | 190 |

## 10. Holds

- Numeric infra/cap/risk KPI scores (no methodology)
- bf_* Fit preference fields
- Portfolio % / geo census fields
- Named Leadership Team without current official sources
- Tafer section deepen held for Coral Beach assignment integrity
- Country taxonomy expansion (optional; not applied)

## 11. Numeric fields intentionally blank

`infra_kpi_*`, `cap_kpi_*`, `*_signal_*`, `locationType*`, `*Experience`, `geo_*` — **HOLD / recommend deprecate or rubric-first redesign**, never invent scores.

## 12. Production section matrix after

`reports/operator-setup-production-section-matrix-after.md` — Empty **0**.

## 13. Operator spot checks

`reports/operator-setup-phase-d-spot-checks.md`

| Operator | Verdict |
| -------- | ------- |
| Hotel Equities / Arbor | Full retained Complete (materials Partial = presentation) |
| GHL | Complete except Infrastructure Partial (thin tech posture; no invented vendors) + materials |
| Tafer | Core 1:1 Complete; section tables Partial (held) |
| Presidente / Highgate / Santa Fe / Aimbridge / Marriott / Hilton / Accor / IHG / Iberostar / Remington / OxoHotel / Brittain | Core Complete; Infrastructure typically Partial (brand-dependent / diligence confirm); Leadership Team N/A |

## 14. Remaining Partial — exact reasons

| Pattern | Reason |
| ------- | ------ |
| Infrastructure Partial (most Prod) | Thin OE posture rows only; no invented PMS/CRS vendor stack; KPI scores held |
| materials Partial/N/A | Presentation assets — not Setup company-truth |
| leadershipTeam N/A | Named people require current official sources (REDESIGN) |
| Profile Partial (Barceló, Meliá, Mandarin, Shangri-La, Sonesta, Auberge, Four Seasons, Rosewood, Hyatt, …) | OE thin description only; deep packs still missing |
| Commercial Partial (some luxury / thin OE) | Engagement + structures evidence thin; bf_* intentionally blank |
| brandRel Partial (some) | Fewer BR intel rows than Complete bar |
| Tafer section Partials | Coral Beach hold — do not fabricate Assignment-backed section packs |
| Leadership Platform Partial (few) | <3 capability rows after markets/languages |

## 15. KEEP / NARROW / REDESIGN / DEPRECATE

| Treatment | Tables |
| --------- | ------ |
| **KEEP** | Profile; Platform & Markets; Operating Platform; Engagement; Infrastructure; Brand Relationships |
| **NARROW** | Commercial Fit (drop bf_* as company truth); Governance (narrative only; no fake KPIs); Leadership Platform (capability/markets, not people); Explorer Materials (presentation) |
| **REDESIGN** | Leadership Team Members (sourced people registry with verify date, or drop from Setup completeness) |
| **DEPRECATE** (as product SoT) | Case Studies; Diligence QA |

## 16. Overall Setup architecture verdict

Setup is now a **Production-usable company fact + OE-derived narrative layer**, not a golden/fixture-only artifact. Completeness must be measured on **section coverage for 36 Production**, never fixture row density.

## 17. Ready to hand off to Fit?

**Conditional yes for Fit adapter remap (shadow only).** Do not treat blank bf_* / KPI scores as Fit blockers. Full Fit v2.1 productization after founder accepts NARROW/REDESIGN/DEPRECATE decisions above.

## 18. Exact founder decisions

1. Accept NARROW Commercial (no bf_* as company truth)  
2. Accept HOLD on infra/cap KPI numeric scores (deprecate or rubric later)  
3. Accept Leadership Team N/A without verified names (REDESIGN)  
4. Timing to deprecate Case Studies / Diligence QA as product SoT  
5. Optional country taxonomy expansion  
6. Authorize Path A — Fit Adapter Remap + Fit v2.1 Shadow  

## 19. Recommended next phase

**Path A — Fit Adapter Remap + Operator Fit v2.1 Shadow** (no live ranking cutover until shadow validated).

---

## Confirmation

- No Operator Fit / weights / CRI / ranking code changed  
- Owner pilot remains disabled  
- Webhound not merged  
- No fixture prose cloned to Production  
