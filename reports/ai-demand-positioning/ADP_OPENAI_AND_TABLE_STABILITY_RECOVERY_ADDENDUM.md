# OpenAI Provider + Table Stability — ADP Recovery Addendum

**Date:** 2026-08-21  
**Mode:** FINISH / RECOVER — correctness only (no redesign)  
**Artifacts:**  
- `reports/ai-demand-positioning/adp-openai-subject-false-negative-audit-v1.json` (initial automated pass; see corrected conclusion below)  
- `scripts/run-adp-openai-subject-false-negative-audit-v1.mjs`  
- `scripts/reparse-adp-subject-mentions-v1.mjs`  
- `scripts/test-adp-subject-name-matching-v1.mjs`

---

## OPENAI PROVIDER AUDIT

Subject presence rates on certified/published periods (parsed `mentioned` / provider observations):

| Property | OpenAI | Gemini | Perplexity | Claude |
|----------|--------|--------|------------|--------|
| Waterstone | **43.6%** | 65.4% | 38.5% | 44.9% |
| Renaissance TS | **29.2%** (highest) | 9.2% | 18.5% | 13.8% |
| Cambridge Beaches | **51.7%** | 73.3% | 71.7% | 78.3% |
| NOW NOW NOHO | **82.5%** (highest) | 20.6% | 27.0% | 12.7% |
| Hotel Phillips | **1.6%** | 42.9% | 47.6% | 39.7% |

Founder “OpenAI looks low” is **property-specific**, not universal. The extreme case is **Hotel Phillips**.

---

## OPENAI FALSE NEGATIVES

Strict raw-needle audit (alias / name patterns in raw OpenAI text while stored `mentioned=false`):

| Property | OpenAI absent | Raw contains subject alias | True false negatives |
|----------|---------------|----------------------------|----------------------|
| Waterstone | 44 | 0 | **0** |
| Renaissance | 46 | 0 | **0** |
| Cambridge | 29 | 0 | **0** |
| NOW NOW | 11 | 0 | **0** |
| Phillips | 62 | 2 | **2** |

### Phillips false negatives (examples)

| Scenario | Raw OpenAI Name | Canonical | Current | Correct | Cause |
|----------|-----------------|-----------|---------|---------|-------|
| `std_kc_biz_09` | The Phillips Hotel | Hotel Phillips Kansas City, Curio… | ABSENT | PRESENT | alias missing (`The Phillips Hotel`) |
| `prop_hpkc_14` | The Phillips Kansas City, Curio Collection by Hilton | same | ABSENT | PRESENT | alias / punctuation variant |

After governed alias + normalized matching: OpenAI Phillips **1.6% → 4.8%** (1→3 of 63). Still far below ~40% peers.

---

## ENTITY ROOT CAUSE

**Pipeline today:** `detectPropertyMention` → lowercase/`&→and` normalized substring against `buildNameVariants` (name, identityAliases, limited short forms, affiliation). **No** fuzzy score, **no** competitor registry for subject.

**Suppressing OpenAI?** Only for Phillips, and only **2** scenarios — not the driver of the 1.6% vs 40% gap. Auto short-forms like `Renaissance New York` were a Midtown collision risk → guarded so Times Square subject short-forms must retain `times square`.

---

## METRIC IMPACT (Phillips reparse, no new LLM calls)

| Metric | OLD | NEW |
|--------|-----|-----|
| OpenAI presence (obs) | 1.6% (1/63) | 4.8% (3/63) |
| Demand Capture / Scenario Presence | 77.8% | 77.8% (unchanged — other providers already covered scenarios) |
| Consideration (all providers) | 34% | 34% (2 flips negligible in ~244 obs) |

**Runtime observations:** reparse `--apply` executed for Phillips period `…21bf47`. **Published snapshot not yet rebuilt** — customer JSON still shows pre-reparse until republish.

---

## PROVIDER DIFFERENCE CONCLUSION

### **MIXED → predominantly REAL PROVIDER DIFFERENCE**

- **Phillips:** Real OpenAI under-recommendation (≈40pp gap remains after alias fix). Parsing defect is real but small (+2 scenarios).  
- **Cambridge:** OpenAI lower than peers with **0** false negatives → real.  
- **Waterstone:** OpenAI mid-pack; Perplexity lower — not an OpenAI-only artifact.  
- **Renaissance / NOW NOW:** OpenAI is the **highest** provider — contradicts a global “OpenAI is low” hypothesis.

Do **not** normalize providers to look similar.

---

## TABLE STABILITY AUDIT

| Surface | Issue |
|---------|--------|
| `#adpCompTable` | Auto layout → column widths shifted with hotel-name length / territory filter |
| `#adpCompCount` under Demand Territory | CORE note tied to filter; height/layout jumped when Overall hid the note |

---

## TABLE FIX

- `table-layout: fixed` + `<colgroup>` role widths on Competitive Overview table  
- Stable min-height on Demand Territory filter chrome  
- Filter area contains **only** the territory control (no CORE metadata)

---

## CORE NOTE RELOCATION

| Before | After |
|--------|-------|
| Under Demand Territory dropdown (`#adpCompCount`) | Competitive Overview title row: **Competitive Overview** + muted `Based on N CORE comparable hotels` (`#adpCompCoreMeta`) |

Click-to-highlight CORE rows preserved. Owner + share HTML updated.

---

## CODE / TESTS

- Subject match v2: `response-parser.js` (`normalizeSubjectHaystack`, expanded aliases, location-token guard)  
- Phillips `identityAliases` extended  
- `npm run test:adp-subject-name-matching-v1`  
- Reparse: `node scripts/reparse-adp-subject-mentions-v1.mjs --property adp_hotel_phillips_kansas_city --apply`

**Still pending founder OK:** republish Phillips (and optionally all five) so customer payloads pick up mention flips + prior recovery entity/impact fixes.
