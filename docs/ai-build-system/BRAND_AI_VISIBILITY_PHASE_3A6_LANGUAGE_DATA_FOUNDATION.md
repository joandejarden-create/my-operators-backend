# Brand AI Visibility — Phase 3A.6 Language Data Foundation

> **Status:** COMPLETE · PASS · 2026-08-14  
> **Prior:** `BRAND_AI_VISIBILITY_PHASE_3A5_SHOWCASE_MULTILINGUAL_DESIGN_AUDIT.md`  
> **Hard stop honored:** 0 provider calls · 0 Spanish production prompts · 0 deploys · Airtable schema APPLY deferred  
> **BUILD STATUS:** `BRAND_AI_VISIBILITY_PHASE_3A6_LANGUAGE_DATA_FOUNDATION_PASS`  
> **Next:** `PHASE_3A7_BILINGUAL_PROMPT_GOVERNANCE`

---

# BRAND_AI_VISIBILITY_PHASE_3A6_LANGUAGE_DATA_FOUNDATION_COMPLETE

## 1. Constitution Compliance

Preserved: no arbitrary GEO/confidence score; no fake history; Company Validated untouched; centralized monitoring; viewer→auth / subject→analysis; Provider + Geography + Language first-class; evidence / interpretation / action separated; no provider blending; no language blending; UNKNOWN ≠ NOT ELIGIBLE; Explorer baselines untouched; no deploy.

## 2. Canonical Language Model

```
INTERNAL_VALUES: en | es
DISPLAY_VALUES: English | Spanish
LOCALE_SEPARATE: YES (never es-MX / en-US as language)
NORMALIZATION: lib/ai-visibility/language-dimension.js
  (normalizeLanguage, isSupportedAiVisibilityLanguage, getLanguageDisplayLabel,
   requireSupportedLanguage, resolveReadLanguage, buildLanguageFilterContract)
```

## 3. Architecture Before / After

| Dimension | Before | After |
|-----------|--------|-------|
| PROMPT_LANGUAGE | MISSING (implicit EN) | Runtime `language` + optional `semanticPairId`; Airtable fields proposed (APPLY deferred) |
| FINGERPRINT_LANGUAGE | MISSING | `ai_visibility_prompt_cohort_v2` includes language + semanticPairId |
| BATCH_LANGUAGE | MISSING | Explicit language; language-homogeneous batches |
| RUN_LANGUAGE | MISSING | Declared monitoring language on run |
| RAW_RESPONSE_LANGUAGE | MISSING | Declared/requested language stored (no post-hoc inference required) |
| MENTION_LANGUAGE | MISSING | Resolvable via evidence/run language |
| CITATION_LANGUAGE | MISSING | Source aggregation language-filtered |
| EVIDENCE_LANGUAGE | MISSING | `language` on evidence; store + read filters |
| METRIC_LANGUAGE | MISSING | Snapshot query filter + identity includes language |
| TREND_LANGUAGE | MISSING | `NON_COMPARABLE_LANGUAGE` on mismatch |
| API_LANGUAGE | MISSING | `?language=en\|es` on Brand read endpoints |
| READ_SERVICE_LANGUAGE | MISSING | Portfolio, Exec, Overview, Trend, Questions, Competitors, Sources, Evidence, HDV |
| UI_FILTER_LANGUAGE | MISSING | Shared contract; selector visible only when >1 language |

## 4. Semantic Pair Foundation

```
SEMANTIC_PAIR_ID_SUPPORTED: YES (runtime)
LANGUAGE_IN_PAIR_IDENTITY: YES
TEXT_MUST_BE_LITERAL_TRANSLATION: NO
PAIR_VALIDATION_RULES:
  same semanticPairId; languages en+es; same intentTerritory;
  same geographyScope/commercialRegion/country; same promptFamily;
  same entityScope/peerSetId; compatible stakeholderRelevance;
  prompt text may differ naturally
```

## 5. Legacy Monitoring

```
LEGACY_BATCH_COUNT: 7
LEGACY_RUN_COUNT: 36
SAFE_EN_BACKFILL: YES (all 7 batches; 1367 records missing language; 0 Spanish text hints)
AMBIGUOUS: 0
DRY_RUN_RESULT: SAFE_TO_BACKFILL_EN=1367 UNSAFE=0
APPLIED: 1367 (additive language=en + auditable languageBackfill; checkpoints under data/ai-visibility/legacy-language-backfill-checkpoints)
CONTENT_CHANGED: NO
TIMESTAMPS_CHANGED: NO
METRICS_CHANGED: NO
```

## 6. Storage

```
LANGUAGE_FIRST_CLASS: YES
QUERY_FILTER: provider + geography + language (+ entity/intent/metric)
SNAPSHOT_IDENTITY: includes language
EVIDENCE_IDENTITY: includes language
SOURCE_AGGREGATION: language-pure (no cross-language merge by default)
```

## 7. Trend Comparability

Key: `provider × geographyKey × language × semanticPairId (or prompt family/version) × peerSetVersion × metricVersion × compatibleEntityUniverse`

```
CROSS_LANGUAGE_TREND_ALLOWED: NO
```

## 8. APIs

Language-aware: portfolio, executive-summary, overview, trend, questions, competitors, sources, evidence.

```
SILENT_LANGUAGE_FALLBACK: NO
UNMONITORED_LANGUAGE_STATE: not_monitored (display Not Monitored)
```

## 9. Language Availability (current real data after backfill)

```
GLOBAL: English
CALA: English
EUROPE: English
NORTH_AMERICA: English
MEXICO: none (until showcase wave)
```

## 10. UI Foundation

```
FILTER_CONTRACT_READY: YES
VISIBLE_NOW: NO (hidden until >1 language for selected geography)
SHOW_WHEN_MULTIPLE_LANGUAGES: YES
ALL_LANGUAGES_OPTION: NO
PRODUCT_DEFAULT_WHEN_MULTIPLE: English (explicit UI defaultSelection only)
```

## 11–12. Executive Summary / Detailed View

Both language-aware underneath; all sections scoped to selected language; no cross-language aggregation.

## 13. Metrics

```
METRIC_FORMULAS_CHANGED: NO
METRIC_VERSION_CHANGED: NO (remains ai_visibility_metrics_v1)
LANGUAGE_SCOPE_ADDED: YES
```

## 14. Airtable Prompt Governance

```
LANGUAGE_FIELD: proposed (English|Spanish → runtime en|es)
SEMANTIC_PAIR_FIELD: proposed
SCHEMA_WRITES: 0
DEFERRED_TO_PHASE_3A7: YES
```

## 15. Spanish Prompt Style

```
GUIDE_CREATED: YES
PATH: docs/ai-build-system/AI_VISIBILITY_SPANISH_HOSPITALITY_PROMPT_STYLE.md
```

## 16. Provider Compatibility

```
OPENAI: YES
GEMINI: architecture-ready (independent dimension)
PERPLEXITY: architecture-ready
ALL_AI: NO
ALL_LANGUAGES: NO
```

## 17. Showcase Architecture Readiness

```
GLOBAL_EN: READY
CALA_EN: READY
CALA_ES: ARCHITECTURE_READY (no Spanish prompts yet)
EUROPE_EN: READY
NA_EN: READY
MEXICO_EN: ARCHITECTURE_READY (no Mexico monitoring yet)
MEXICO_ES: ARCHITECTURE_READY
```

## 18. Future Showcase Wave

```
CALLS_PLANNED: 49
CALLS_EXECUTED: 0
```

## 19. Authorization

```
UNCHANGED: YES
CROSS_TENANT: denied with language param
DIRECT_ID_SWAP: denied with language param
LANGUAGE_BYPASS: NO
```

## 20–26. See completion return in chat / agent response.

**Next recommended phase:** `PHASE_3A7_BILINGUAL_PROMPT_GOVERNANCE`
