# Scenario Peer Eligibility V1

> **Version:** `scenario_peer_eligibility_v1`  
> **Fixture universe:** `fixtures/ai-visibility/benchmark-eligible-universe-v1.json`  
> **Engine:** `lib/ai-visibility/competitive-moat/scenario-peer-eligibility.js`  
> **Scenarios:** governed CORE panel in `fixtures/ai-visibility/scenario-registry-v1.json` — do not invent scenarios.

## Commercial relations

| Relation | Meaning | In benchmark calculation? |
|----------|---------|---------------------------|
| CORE | Highly plausible alternative in this exact owner decision | Yes — **denominator** |
| SECONDARY | Credible, less direct | **No** (ADDITIONAL_OBSERVED_CONTEXT only; CORE-first policy) |
| CONDITIONAL | Only if a stated asset/market/development condition is met | No, unless condition is satisfied |
| NON_COMPARABLE | Not commercially appropriate | No |

**MEASUREMENT GAP ≠ NON-COMPARABLE.** A CORE peer with no stored scenario grains is `MEASUREMENT_COVERAGE_GAP`.

## Soft collection (affiliation)

CORE clique: Autograph, Curio, Tribute, Tapestry, **Vignette**, Ascend.

SECONDARY platforms: Handwritten, MGallery, Preferred, Radisson Individuals.

CONDITIONAL: Trademark, BW Premier, BW Signature, SLH, Design Hotels (independent/soft affiliation without major-flag loyalty as the primary job).

Lifestyle flags (Indigo, Kimpton, Canopy, Tempo, AC, Voco, Even, Radisson RED) and hard UU (Westin, Radisson Blu, DoubleTree, Radisson) are NON_COMPARABLE here.

Vignette is eligible because it is customer-visible and commercially a collection peer — **not** because it was on peer v5 (it was not).

## Lifestyle

CORE: Hotel Indigo, Kimpton, Canopy, Tempo, **Voco**, AC Hotels.

SECONDARY: Design Hotels, Radisson RED.

CONDITIONAL: Preferred Hotels & Resorts (affiliation network, not a lifestyle flag).

NON_COMPARABLE: Even Hotels (wellness/select — different owner decision than lifestyle individuality).

Collection brands are not forced into lifestyle merely because they are upper-upscale.

## Hard-brand upper-upscale

CORE: Westin, Radisson Blu, DoubleTree, Radisson.

AC may be SECONDARY on chain-scale / market-entry. Collections are not the Westin set. **No 22-brand fallback.**

## Owner flexibility

CORE: Ascend, Radisson Individuals, Trademark, Handwritten, Preferred, Vignette.

SECONDARY: Autograph, Curio, Tapestry, SLH, BW Premier.

Radisson family members are **not** interchangeable: Blu ↔ Radisson CORE on hard UU; Blu ↔ RED NON_COMPARABLE; Individuals ↔ Blu NON_COMPARABLE on soft-brand.

## Conversion

Independent conversion and conversion suitability reuse collection logic, and add lifestyle-to-lifestyle and hard-to-hard CORE sets. Voco may be CONDITIONAL vs collections (upscale conversion, not collection model).

## Symmetry

CORE/SECONDARY pairs under the same scenario are symmetric. `ASYMMETRIC_UNJUSTIFIED` must be 0 before production certification.

## Named mandatory cores

A stack of SECONDARY brands cannot replace missing named cores:

- Autograph soft-brand: Curio + Vignette
- Curio soft-brand: Autograph + Vignette
- Indigo lifestyle: Kimpton + Voco
- Westin chain-scale / new-build: Radisson Blu
- Ascend owner-flex: Radisson Individuals + Vignette
