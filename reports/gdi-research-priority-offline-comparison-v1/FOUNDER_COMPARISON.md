# GDI research-priority offline comparison V1 (Option 2)

Generated: 2026-11-19T12:27:54.090Z

## Safety
- Paid calls: 0
- Airtable writes: 0
- Production selection policy changed: **false**
- Founder Option 2 selected for evaluation: **true**
- Production scoring weights: **unchanged**

## Structural finding
With fairMinPerSegment=1 and maxCandidates < planned segments, commercial score fill never runs; late taxonomy segments are systematically deferred.

## Coverage distinction
- Shallow discovery coverage ≠ deep-research coverage
- Low research expenditure is not treated as superior quality without commercial outcome evidence.

## Edition dedupe (Bethesda)
- Inventory rows: 86
- After org+edition dedupe: 84
- Dropped same-edition duplicates: 2
- Distinct future editions of the same org are **not** collapsed

## Policy quality comparison (Bethesda, cap=8)

| Policy | Selected | Segment diversity | Mean score | With evidence | With contact name |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current fair-floor | 8 | 8 | 59.3 | 4 | 6 |
| Pure commercial | 8 | 3 | 72.96 | 8 | 8 |
| Fair 25% / Commercial 75% | 8 | 4 | 69.67 | 7 | 8 |
| Fair 40% / Commercial 60% | 8 | 5 | 68.41 | 6 | 8 |
| Fair 50% / Commercial 50% | 8 | 5 | 68.41 | 6 | 8 |

### Option 2 arm slot budgets
- **fair25_commercial75**: fairSlots=2, commercialSlots=6, diversity=4
- **fair40_commercial60**: fairSlots=3, commercialSlots=5, diversity=5
- **fair50_commercial50**: fairSlots=4, commercialSlots=4, diversity=5

### Fair rotation (fair40 / 4 cycles)
- Late segments recovered across cycles: **false**
- Starved after 4 cycles: projects_infrastructure, social_special_events, local_catering_day_meetings, corporate_celebrations_private_events, travel_trade_group_intermediaries, relocation_extended_stay, preferred_lodging_partnerships, professional_event_planners

### Overlap
- Current ∩ pure commercial: 1
- Option2 40/60 ∩ current: 3
- Option2 40/60 ∩ pure: 6

### Examples overlooked by current fair-floor cap (pure commercial top)
- American Medical Women's Association (AMWA) · associations_conferences_events · proposed=75.5 existing=53.08
- American Medical Women's Association (AMWA) · associations_conferences_events · proposed=73 existing=51.13
- Association for Clinical and Translational Science (ACTS) · education_scientific · proposed=73 existing=41.58
- National Down Syndrome Society (NDSS) · associations_conferences_events · proposed=72.6 existing=80.25
- AHIMA · associations_conferences_events · proposed=72.2 existing=80.25
- American College of Cardiology (ACC) · associations_conferences_events · proposed=72.2 existing=82.2
- Arlington Soccer Association · associations_conferences_events · proposed=71.4 existing=81.22

## Recommendation
Founder selected Option 2 (bounded fairness then commercial priority). Keep production weights unchanged until this offline evaluation is reviewed. Prefer fair40/commercial60 or fair25/commercial75 so commercial fill can run inside the deep cap; enable multi-cycle segment rotation so late taxonomy segments are not permanently starved. Do not treat low spend as quality.

**Founder gate:** keep production weights unchanged until this Option 2 evaluation is reviewed. Do not enable a new paid production selection policy from this report alone.
