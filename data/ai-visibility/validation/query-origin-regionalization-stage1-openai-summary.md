# Query-Origin Regionalization — Stage 1 OpenAI

**Experiment:** QUERY_ORIGIN_REGIONALIZATION_EXPERIMENT_V1 / STAGE_1_OPENAI
**Asset geography:** Mexico
**Model:** gpt-4.1
**Calls:** 192 success / 0 failed of 192 planned
**Actual cost (est.):** $30.98
**Execution seed:** query_origin_stage1_openai_20260815

## Methodology

- Identical Mexico-asset prompt text across four query origins
- Only OpenAI `web_search.user_location` changes
- 8 families × EN+ES × 4 origins × 3 repeats
- Metrics: Presence / brand-set / associated source domains only (no Recommendation / First)
- No Regionalization Score

## Presence Differences

- **Handwritten Collection**: MAX_MIN_PRESENCE_DELTA = 18.8 pp — NEW_YORK 2/48; MIAMI 0/48; MEXICO_CITY 3/48; MADRID 9/48
- **Kimpton Hotels**: MAX_MIN_PRESENCE_DELTA = 16.7 pp — NEW_YORK 21/48; MIAMI 13/48; MEXICO_CITY 19/48; MADRID 20/48
- **Trademark Collection by Wyndham**: MAX_MIN_PRESENCE_DELTA = 14.6 pp — NEW_YORK 3/48; MIAMI 1/48; MEXICO_CITY 8/48; MADRID 5/48
- **Autograph Collection**: MAX_MIN_PRESENCE_DELTA = 12.5 pp — NEW_YORK 38/48; MIAMI 32/48; MEXICO_CITY 35/48; MADRID 36/48
- **Small Luxury Hotels of the World**: MAX_MIN_PRESENCE_DELTA = 12.5 pp — NEW_YORK 6/48; MIAMI 3/48; MEXICO_CITY 7/48; MADRID 9/48
- **Curio Collection by Hilton**: MAX_MIN_PRESENCE_DELTA = 10.4 pp — NEW_YORK 34/48; MIAMI 33/48; MEXICO_CITY 32/48; MADRID 37/48
- **Hotel Indigo**: MAX_MIN_PRESENCE_DELTA = 10.4 pp — NEW_YORK 17/48; MIAMI 12/48; MEXICO_CITY 17/48; MADRID 17/48
- **Tribute Portfolio**: MAX_MIN_PRESENCE_DELTA = 10.4 pp — NEW_YORK 26/48; MIAMI 22/48; MEXICO_CITY 25/48; MADRID 27/48
- **Tapestry Collection by Hilton**: MAX_MIN_PRESENCE_DELTA = 10.4 pp — NEW_YORK 26/48; MIAMI 21/48; MEXICO_CITY 25/48; MADRID 24/48
- **Vignette Collection**: MAX_MIN_PRESENCE_DELTA = 10.4 pp — NEW_YORK 17/48; MIAMI 12/48; MEXICO_CITY 17/48; MADRID 15/48

## Brand-Set Differences

- **NEW_YORK**: brand-set size 25; unique-to-origin: (none)
- **MIAMI**: brand-set size 26; unique-to-origin: Aimbridge Hospitality (LATAM)
- **MEXICO_CITY**: brand-set size 29; unique-to-origin: BW Signature Collection
- **MADRID**: brand-set size 27; unique-to-origin: (none)

Pairwise overlap N:
- NEW_YORK ∩ MIAMI = 23
- NEW_YORK ∩ MEXICO_CITY = 25
- NEW_YORK ∩ MADRID = 25
- MIAMI ∩ MEXICO_CITY = 25
- MIAMI ∩ MADRID = 24
- MEXICO_CITY ∩ MADRID = 27

## Source Differences

Top associated/cited source domains by origin:
- **NEW_YORK**: practiceguides.chambers.com (11); en.wikipedia.org (9); elfinanciero.com.mx (8); hoteldevelopmentguide.com (8); hospitalitas.com.mx (8)
- **MIAMI**: en.wikipedia.org (11); hoteldevelopmentguide.com (10); practiceguides.chambers.com (9); development.wyndhamhotels.com (8); hospitalitas.com.mx (8)
- **MEXICO_CITY**: hospitalitas.com.mx (10); hoteldevelopmentguide.com (9); en.wikipedia.org (9); elfinanciero.com.mx (7); practiceguides.chambers.com (7)
- **MADRID**: practiceguides.chambers.com (10); en.wikipedia.org (10); hospitalitas.com.mx (9); elfinanciero.com.mx (9); hoteldevelopmentguide.com (9)

Domains unique to origin (sample, first 5):
- **NEW_YORK**: hosteltur.com, referencerealestate.mx, accor-residences.com, argaamplus.s3.amazonaws.com, gtmwest.com
- **MIAMI**: baystreethospitality.com, downloads.regulations.gov, sec.gov, publications.hvs.com, mlhcollection.com
- **MEXICO_CITY**: d25wybtmjgh8lz.cloudfront.net, mazatlaninteractivo.com.mx, udekom.org.rs, fuerte-group.com, press.accor.com
- **MADRID**: smartmeetings.com, rosewoodhotels.com, sisinternational.com, worldconstruccion.mx, namronhospitality.com

## Repeatability

Materiality gate for Stage 1 decision: MAX_MIN ≥ 25 pp with cross-repeat / cross-prompt consistency.
materialBrandDeltaCount=0; consistentDifferenceCount=0

- **Handwritten Collection**: Δ 18.8 pp → OBSERVED_ONCE
- **Kimpton Hotels**: Δ 16.7 pp → OBSERVED_ONCE
- **Trademark Collection by Wyndham**: Δ 14.6 pp → OBSERVED_ONCE
- **Autograph Collection**: Δ 12.5 pp → OBSERVED_ONCE
- **Small Luxury Hotels of the World**: Δ 12.5 pp → OBSERVED_ONCE
- **Curio Collection by Hilton**: Δ 10.4 pp → OBSERVED_ONCE
- **Hotel Indigo**: Δ 10.4 pp → OBSERVED_ONCE
- **Tribute Portfolio**: Δ 10.4 pp → OBSERVED_ONCE

## Decision

**NO_MEANINGFUL_REPEATABLE_DIFFERENCE**

Next: QUERY_ORIGIN_REMAINS_RESEARCH_ONLY

## Limitations

- OpenAI-only; Claude replication not run
- Probabilistic LLM outputs — differences need replication before causal claims
- Research only — not production Query-Origin Geography
- Wording: associated/cited source domains only (not "influencing sources")
