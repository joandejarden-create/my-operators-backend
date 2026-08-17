# Brand AI Visibility — Phase 3A.5 Showcase + Multilingual Design Audit

> **Status:** Design-only / read-only complete · 2026-08-14  
> **Supersedes for language scope:** extends `BRAND_AI_VISIBILITY_PHASE_3A5_SHOWCASE_DESIGN_AUDIT.md`  
> **Activity:** 0 provider calls · 0 writes · 0 deploys  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A5_SHOWCASE_MULTILINGUAL_DESIGN_AUDIT_PASS`  
> **Next:** `PHASE_3A6_LANGUAGE_DATA_FOUNDATION`

---

## Critical finding — Language

**IS_LANGUAGE_CURRENTLY_STORED_AS_A_FIRST_CLASS_DIMENSION? NO**

Verified across:

| Layer | Status |
|-------|--------|
| Prompt seed / Airtable prompt field specs | **MISSING** (no Language field) |
| Batch / run / evidence / metric snapshot | **MISSING** |
| Cohort fingerprint / trend comparability | **MISSING** (language not in key) |
| API filters / Exec / Detail | **MISSING** |
| UI Language filter | **MISSING** |

All current live monitoring is **implicit English only** (prompt text is English; not declared).

Spanish monitoring must **not** start until language is first-class — otherwise Spanish runs would silently mix into English trends.

---

## Showcase model (locked)

- **Company Portfolio** ≠ **Competitive Cohort**
- One shared monitored universe; authorization changes the view
- **Cohort membership ≠ decision eligibility** (e.g. Westin in cohort but not Soft Brand prompts)
- **Addressability ≠ Language** (eligibility language-neutral; metrics language-pure)
- **No All Languages / blended language metrics**
- Canonical languages: `en` / `es` (display English / Spanish); locale stays in geography (`Mexico`), not `es-MX`

---

## Portfolios & cohort (unchanged recommendation from live Brand Basics)

### Marriott (5)
Autograph `recEJCTDj1zrsjPM6` · Tribute `recCvV0PuZOi8c3hC` · Design Hotels `rec02zPClpWUTCyXM` · Westin `recIPuBC50fv13zRR` · AC Hotels `rec9aZp7GHtzUEg0c`  
Excluded: Renaissance / Le Méridien / JW / Edition / W (**not Active** in Brand Basics). Courtyard (scale quality). Flagship Marriott Hotels / Sheraton (too broad).

### Hilton (4)
Curio · Tapestry · Canopy · Tempo  
Excluded: Hilton Hotels flagship, DoubleTree, select-service lower brands.

### Choice (4)
Ascend · Radisson Individuals · Radisson Blu · Radisson RED  
Excluded: midscale/economy. **Radisson Collection** not Active.

### Shared cohort (15)
Same as prior audit: Marriott 5 + Curio/Tapestry/Canopy/Tempo + Ascend/Individuals/Blu + Indigo + Kimpton + MGallery.  
**CAN_ONE_SHARED_COHORT: YES**  
Peer set action: **VERSION_NEW** → `peers_uu_collection_lifestyle_owner_decision_v2`

### Intent eligibility constraints (examples)
| Brand | In cohort | Soft Brand / Collection prompts | UU Positioning | Lifestyle |
|-------|-----------|----------------------------------|----------------|-----------|
| Autograph / Tribute / Design / Curio / Tapestry / Ascend / Individuals / MGallery | Yes | Eligible | Eligible | Partial |
| Canopy / Tempo / AC / Indigo / Kimpton / RED | Yes | Partial | Eligible | Eligible |
| Westin / Radisson Blu | Yes | **Not eligible** (hard brand) | Eligible | Low |

---

## Intent taxonomy (7)

1. Conversion  
2. Collection / Soft Brand  
3. Lifestyle Positioning  
4. Upper-Upscale Positioning  
5. New Build  
6. Branded Residences / Mixed Use  
7. Owner Economics / Flexibility  

DEFER: Operator Selection / HMA · Distribution/Loyalty claims · Development Credibility track-record claims.

---

## Language strategy by geography

| Geography | Wave 1 languages |
|-----------|------------------|
| Global | `en` only |
| Europe | `en` only |
| North America | `en` only |
| CALA | `en` + `es` |
| Mexico | `en` + `es` |

CALA Spanish and Mexico Spanish share **semantic families** with different geographic framing (`semanticPairId` + geography + language).

### Spanish style guide (design)
- Professional hotel investment/development Spanish
- Not literal EN→ES
- Keep established English industry terms when bilingual pros retain them: *upper-upscale, soft brand, lifestyle, branded residences, PIP* as needed
- Prefer natural: *conversión, marca, propietario, desarrollador, colección, franquicia* where idiomatic
- No dedicated Dealality Spanish hospitality glossary exists today → **create lightweight glossary in prompt governance phase**

### Semantic pairing pattern
```
promptFamily + decisionTerritory + geographyKey + semanticPairId + language(en|es) + version
```

---

## Language-pure metrics

All observed metrics must filter by language:

AI Presence · Competitive Position · Recommendation Rate/Share · Top-3 · First Rec · Questions Won/Missing · Citation Rate · Decision Visibility Coverage · Owner Intent Coverage · Top/Weakest Territory · Breadth · sources · AI vs Dealality · Review Items

**Comparable trend key must include language:**

`provider + geographyKey + language + semanticPairId|promptFamily + peerSetVersion + metricVersion + compatibleEntityUniverse`

Do **not** compare CALA `en` vs CALA `es` as one series.

### Future-ready only
- Language Visibility Gap (ES − EN presence, pp)
- Cross-Language Recommendation Persistence
- Cross-Language Rank Divergence  

No All Languages. No composite score.

---

## Addressable Decision Universe

**CAN_BUILD_NOW: PARTIAL** · `addressable_decision_universe_v1`  
Required: Active/Live · Parent · Chain Scale · Brand Model · prompt chainScale/developmentType/intent  
UNKNOWN ≠ NOT ELIGIBLE  
Language does **not** gate addressability.

---

## Monitoring wave (design only)

Execution unit verified: **provider × prompt × geography × language** (full peer universe per response; **not** × brands).

### Full bilingual option (7 territories)

| Cell | Calls |
|------|------:|
| GLOBAL_EN | 7 |
| CALA_EN | 7 |
| CALA_ES | 7 |
| EUROPE_EN | 7 |
| NORTH_AMERICA_EN | 7 |
| MEXICO_EN | 7 |
| MEXICO_ES | 7 |
| **TOTAL** | **49** |

Historical avg ≈ **$0.68/call** · ≈ **45k tokens/call** (from 36 stored OpenAI runs, ~$24.38).

| Scenario | Calls | LOW | EXPECTED | HIGH |
|----------|------:|----:|---------:|-----:|
| Full bilingual | 49 | ~$26 | ~$33 | ~$49 |
| English-only core (Global+CALA+EU+NA+MX) | 35 | ~$19 | ~$24 | ~$35 |
| Spanish incremental (CALA_ES+MX_ES) | 14 | ~$8 | ~$9.5 | ~$14 |

### Optimized bilingual option
CALA: all 7 EN+ES · Mexico: top 5 territories EN+ES (Conversion, Soft Brand, Lifestyle, UU Positioning, Residences/Mixed Use)

| | Calls | Expected |
|--|------:|---------:|
| Optimized | **45** | ~$30 |
| vs Full | −4 | ~−$3 |
| Tradeoff | Slightly thinner Mexico intent coverage | Acceptable for Wave 1 if Founder prefers |

Mexico strengthens CALA differentiation and bilingual proof for all three parents — **include unless governance blocks** (none found for Active brands above).

### Cadence
**Biweekly** · ~49 calls/period (full) · ~98/month · ~$60–$80/mo expected bilingual  
Prior comparable: after 2 periods · 30-day: ≥2 periods spanning ≥30d · 90-day: ≥3–4 / 90d · language persistence: after ≥2 bilingual periods.

---

## Architecture changes required for language (not implemented)

1. Add `language` (`en`|`es`) to prompt schema + seed + Airtable field  
2. Persist on batch, run, evidence, metric snapshots  
3. Include in cohortFingerprint / comparability key  
4. API query param + portfolio/exec/detail/trend filters  
5. UI Language filter (data-driven options only)  
6. Forbid cross-language aggregation  

---

## Cost / repull decision

**REUSE_EXISTING_PLUS_NEW_SHOWCASE_WAVE**

- Keep existing 2026-08-13 English validation batches as historical (label language=`en` in a future backfill if safe)  
- Do **not** treat them as language-declared until backfilled  
- New showcase wave after language foundation + peer v2 + bilingual prompts  

---

## Build sequence

1. **PHASE_3A6_LANGUAGE_DATA_FOUNDATION** — contract + storage + read filters + comparability  
2. Showcase data governance — portfolios + peer v2 + entitlements design  
3. Prompt governance — EN edits + ES semantic pairs + glossary  
4. Dry-run + cost gate  
5. OpenAI bilingual wave  
6. Reprocess / validation  
7. Company demo entitlements  
8. Founder QA  

---

## Activity

```
LIVE_PROVIDER_CALLS: 0
AIRTABLE_WRITES: 0
SCHEMA_CHANGES: 0
DEMO_ENTITLEMENT_WRITES: 0
PEER_SET_CHANGES: 0
PROMPT_WRITES: 0
DEPLOYS: 0
```
