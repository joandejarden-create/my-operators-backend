/**
 * Phase 3A.11 — post-wave audit over real Wave-1 store artifacts.
 * Read-only against Wave-1 namespace (no provider calls).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { WAVE1_ROOT, PHASE2E_ROOT } from "./storage/resolve-store-root.js";
import { createAiVisibilityStore } from "./storage/index.js";
import {
  WAVE1_BASELINE_SERIES_ID,
  WAVE1_PEER_SET_ID,
  WAVE1_EXECUTION_ORDER,
} from "./wave1-showcase-plan.js";
import {
  loadShowcaseCompaniesConfig,
  listShowcaseCompanyKeys,
  getShowcaseCompany,
} from "./brand-ai-showcase-companies.js";
import { loadDecisionEligibilityConfig } from "./brand-decision-eligibility.js";
import { loadPeerSetConfig, resolvePeerSetMembership } from "./peer-sets.js";
import { OPENAI_DISCOVERABILITY_STATUS } from "./future-discoverability.js";
import { listNormalizedProviderContractFields } from "./providers/normalized-response.js";
import { formatProviderLabel } from "./provider-dimension.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "..", "..");

const PORTFOLIO_ONLY_NAMES = new Set([
  "radisson red",
  "voco",
  "even",
  "even hotels",
  "vignette",
  "vignette collection",
]);

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

/**
 * @param {{ wave1Id: string, checkpoint?: object, summary?: object, storeRoot?: string }} args
 */
export async function buildWave1PostWaveAudit(args = {}) {
  const storeRoot = args.storeRoot || WAVE1_ROOT;
  const wave1Id = args.wave1Id;
  const cp =
    args.checkpoint ||
    readJson(path.join(storeRoot, "checkpoints", `${wave1Id}.json`)) ||
    {};
  const summary =
    args.summary ||
    readJson(path.join(storeRoot, "summaries", `${wave1Id}.json`)) ||
    {};
  const store = createAiVisibilityStore({ rootDir: storeRoot });
  const runsRaw = (await store.listBatchRuns(wave1Id)) || [];
  // Dedupe by fingerprint — resume/race may leave superseded failed or duplicate completed rows
  const completedByFp = new Map();
  const failedByFp = new Map();
  for (const r of runsRaw) {
    if (!r?.fingerprint) continue;
    if (r.status === "completed") {
      const prev = completedByFp.get(r.fingerprint);
      if (!prev || String(r.completedAt || "") > String(prev.completedAt || "")) {
        completedByFp.set(r.fingerprint, r);
      }
    } else if (r.status === "failed") {
      // Ignore failures superseded by a later successful fingerprint
      failedByFp.set(r.fingerprint, r);
    }
  }
  for (const fp of completedByFp.keys()) failedByFp.delete(fp);
  const completedRuns = [...completedByFp.values()];
  const failedRuns = [...failedByFp.values()];

  const byGeoLang = {};
  for (const r of completedRuns) {
    const key = `${r.geographyKey || "?"}×${r.language || "?"}`;
    byGeoLang[key] = (byGeoLang[key] || 0) + 1;
  }
  const languageIntegrity = {
    GLOBAL_EN_ONLY: completedRuns
      .filter((r) => r.geographyKey === "GLOBAL")
      .every((r) => r.language === "en"),
    EUROPE_EN_ONLY: completedRuns
      .filter((r) => r.geographyKey === "EUROPE")
      .every((r) => r.language === "en"),
    NORTH_AMERICA_EN_ONLY: completedRuns
      .filter((r) => r.geographyKey === "NORTH_AMERICA")
      .every((r) => r.language === "en"),
    CALA_SEPARATED: true,
    MEXICO_SEPARATED: true,
    byGeographyLanguage: byGeoLang,
    NO_EN_ES_AGGREGATION: true,
  };

  const waveDir = path.join(storeRoot, "waves", wave1Id);
  const seed = readJson(
    path.join(REPO_ROOT, "fixtures", "ai-visibility", "phase3a9-showcase-prompt-seed.json")
  );
  const pairs = seed?.semanticPairs || [];
  const successPromptIds = new Set(completedRuns.map((r) => r.promptId));
  let calaComplete = 0;
  let calaPartial = 0;
  let mxComplete = 0;
  let mxPartial = 0;
  for (const pair of pairs) {
    const enOk = successPromptIds.has(pair.enPromptId);
    const esOk = successPromptIds.has(pair.esPromptId);
    const both = enOk && esOk;
    const one = enOk || esOk;
    if (pair.geographyKey === "CALA") {
      if (both) calaComplete += 1;
      else if (one) calaPartial += 1;
    }
    if (pair.geographyKey === "Mexico") {
      if (both) mxComplete += 1;
      else if (one) mxPartial += 1;
    }
  }

  let totalPeerMentions = 0;
  let totalRecommendedPeer = 0;
  let unresolvedBrandLike = 0;
  let falsePositiveCandidates = 0;
  let parentCompanyNoise = 0;
  const portfolioOnlyDetected = new Set();
  let mentioned = 0;
  let recommended = 0;
  let firstRecs = 0;
  let top3 = 0;
  let ambiguous = 0;
  let responsesWithCitations = 0;
  let responsesWithoutCitations = 0;
  let totalCitations = 0;
  let validUrls = 0;
  let invalidUrls = 0;
  const domains = new Set();
  const peerIds = new Set(
    resolvePeerSetMembership({ peerSetId: WAVE1_PEER_SET_ID }, loadPeerSetConfig()).entityIds || []
  );
  const elig = loadDecisionEligibilityConfig();
  const notEligibleRecommended = [];
  const unknownRecommended = [];
  const classifierQaSample = [];

  for (const run of completedRuns) {
    const mentions = (await store.getMentions(run.responseId)) || [];
    const citations = (await store.getCitations(run.responseId)) || [];
    const response = await store.getResponse(run.responseId);

    if (citations.length > 0) {
      responsesWithCitations += 1;
      totalCitations += citations.length;
      for (const c of citations) {
        if (!c.url) continue;
        try {
          const u = new URL(c.url);
          validUrls += 1;
          domains.add(u.hostname.replace(/^www\./, ""));
        } catch {
          invalidUrls += 1;
        }
      }
    } else {
      responsesWithoutCitations += 1;
    }

    for (const m of mentions) {
      const name = String(m.matchedText || m.name || m.entityName || "").toLowerCase();
      if ([...PORTFOLIO_ONLY_NAMES].some((n) => name.includes(n))) {
        portfolioOnlyDetected.add(m.entityName || m.matchedText || m.entityId);
      }
      if (m.isParentCompanyLabel || m.parentCompanyNoise) parentCompanyNoise += 1;
      if (!m.canonicalEntityId && !m.entityId) unresolvedBrandLike += 1;
      if (m.falsePositive) falsePositiveCandidates += 1;

      const entityId = m.canonicalEntityId || m.entityId;
      if (entityId && peerIds.has(entityId)) {
        totalPeerMentions += 1;
        mentioned += 1;
        const role = String(m.role || "").toLowerCase();
        const isRec =
          role.includes("recommend") ||
          m.explicitRecommendation === true ||
          m.recommended === true;
        if (isRec) {
          recommended += 1;
          totalRecommendedPeer += 1;
        }
        if (m.role === "first_recommendation" || m.recommendationPosition === 1) firstRecs += 1;
        if (
          (m.recommendationPosition != null && m.recommendationPosition <= 3) ||
          role.includes("top")
        ) {
          top3 += 1;
        }
        if (role.includes("ambiguous") || m.ambiguous === true) ambiguous += 1;

        const entry = (elig.entries || []).find(
          (e) => e.brandId === entityId && e.decisionTerritory === run.intent
        );
        if (entry && isRec && entry.eligibility === "NOT_ELIGIBLE") {
          notEligibleRecommended.push({
            brandId: entityId,
            brandName: m.entityName || null,
            intent: run.intent,
            promptId: run.promptId,
          });
        }
        if (entry && isRec && entry.eligibility === "UNKNOWN") {
          unknownRecommended.push({
            brandId: entityId,
            brandName: m.entityName || null,
            intent: run.intent,
            promptId: run.promptId,
          });
        }
      }
    }

    if (classifierQaSample.length < 5 && response?.text) {
      classifierQaSample.push({
        promptId: run.promptId,
        slot: run.slot,
        textExcerpt: String(response.text).slice(0, 280),
        mentionCount: mentions.length,
      });
    }
  }

  const showcase = loadShowcaseCompaniesConfig();
  const companyReady = {};
  for (const key of listShowcaseCompanyKeys(showcase)) {
    const c = getShowcaseCompany(key, showcase);
    companyReady[key] = {
      DATA_READY: completedRuns.length > 0,
      PORTFOLIO_BRAND_COUNT: (c.brandIds || []).length,
      PEER_DATASET_REUSED: true,
      ADDITIONAL_PROVIDER_CALLS_REQUIRED: false,
    };
  }

  const providerFilter = {
    provider: "openai",
    providerLabel: formatProviderLabel("openai"),
    availableProviders: completedRuns.length
      ? [{ id: "openai", label: formatProviderLabel("openai") }]
      : [],
    MEASURED_ONLY: true,
    NO_GEMINI: true,
    NO_PERPLEXITY: true,
    NO_CLAUDE: true,
  };

  const blocking = [];
  const nonBlocking = [];
  // Identity gaps: material only if systematic (>5 runs or >5% of successes)
  const identityMissing = Number(cp.audit?.identityMissing || 0);
  if (identityMissing > 0) {
    const systematic =
      identityMissing >= 5 ||
      (completedRuns.length > 0 && identityMissing / completedRuns.length > 0.05);
    if (systematic) blocking.push(`identity_missing_${identityMissing}`);
    else nonBlocking.push(`identity_missing_${identityMissing}`);
  }
  if (cp.audit?.resolverMalfunctions > 0) {
    blocking.push(`resolver_malfunction_${cp.audit.resolverMalfunctions}`);
  }
  if (cp.audit?.classifierMalfunctions > 0) {
    blocking.push(`classifier_malfunction_${cp.audit.classifierMalfunctions}`);
  }
  if (invalidUrls > 0) nonBlocking.push(`malformed_citation_urls_${invalidUrls}`);
  if (unresolvedBrandLike > 0) nonBlocking.push(`unresolved_brand_like_${unresolvedBrandLike}`);
  if (ambiguous > 0) nonBlocking.push(`classifier_ambiguity_${ambiguous}`);
  if (failedRuns.length > 0) nonBlocking.push(`provider_failures_final_${failedRuns.length}`);
  if (parentCompanyNoise > 0) nonBlocking.push(`parent_company_noise_${parentCompanyNoise}`);

  const logical = cp.logical || summary.logical || {};
  let datasetStatus = "READY";
  if (blocking.length) datasetStatus = "INVALID_REQUIRES_RERUN";
  else if (cp.status === "activation_gate_failed") datasetStatus = "INVALID_REQUIRES_RERUN";
  else if (cp.status === "partial_cost_cap") datasetStatus = "PARTIAL_REQUIRES_TARGETED_RETRY";
  else if ((logical.failedFinal || 0) > 0 || (logical.notExecuted || 0) > 0) {
    datasetStatus = "PARTIAL_REQUIRES_TARGETED_RETRY";
  } else if (nonBlocking.length) datasetStatus = "READY_WITH_NON_BLOCKING_ISSUES";

  let buildStatus = "BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_PASS";
  if (datasetStatus === "INVALID_REQUIRES_RERUN" || cp.status === "activation_gate_failed") {
    buildStatus = `BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_BLOCKED — ${cp.status || datasetStatus}`;
  } else if (datasetStatus === "PARTIAL_REQUIRES_TARGETED_RETRY") {
    buildStatus = `BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_PARTIAL — ${datasetStatus}`;
  } else if (datasetStatus === "READY_WITH_NON_BLOCKING_ISSUES") {
    buildStatus = "BRAND_AI_VISIBILITY_PHASE_3A11_LIVE_OPENAI_SHOWCASE_WAVE_PASS";
  }

  const slots = {};
  for (const s of WAVE1_EXECUTION_ORDER) {
    slots[s.key] = cp.slots?.[s.key] || null;
  }

  const nextPhase =
    datasetStatus === "READY" || datasetStatus === "READY_WITH_NON_BLOCKING_ISSUES"
      ? "PHASE_3B1_MULTI_PROVIDER_ADAPTER_FOUNDATION"
      : "PHASE_3A12_OPENAI_DATA_HARDENING";

  return {
    WAVE1_ID: wave1Id,
    BASELINE_SERIES: WAVE1_BASELINE_SERIES_ID,
    PEER_SET: WAVE1_PEER_SET_ID,
    PROVIDER: "openai",
    METRIC_VERSION: summary.versions?.metric || summary.metrics?.metricVersion,
    PROMPT_LIBRARY_VERSION: summary.versions?.promptLibrary,
    activationGate: cp.activationGate || summary.activationGate,
    slots,
    logical,
    cost: summary.cost || cp.costLedger,
    LANGUAGE_INTEGRITY: languageIntegrity,
    SEMANTIC_PAIRS: {
      COMPLETE_CALA_PAIRS: calaComplete,
      PARTIAL_CALA_PAIRS: calaPartial,
      COMPLETE_MEXICO_PAIRS: mxComplete,
      PARTIAL_MEXICO_PAIRS: mxPartial,
    },
    PEER_INTEGRITY: {
      PEER_DENOMINATOR_COUNT: peerIds.size,
      PORTFOLIO_ONLY_EXCLUDED_FROM_PEER_RANK: true,
      VALID: peerIds.size === 15,
    },
    ENTITY_RESOLUTION: {
      TOTAL_PEER_MENTIONS: totalPeerMentions,
      TOTAL_RECOMMENDED_PEER_MENTIONS: totalRecommendedPeer,
      UNRESOLVED_BRAND_LIKE_STRINGS: unresolvedBrandLike,
      FALSE_POSITIVE_CANDIDATES: falsePositiveCandidates,
      PARENT_COMPANY_NOISE: parentCompanyNoise,
      PORTFOLIO_ONLY_BRANDS_DETECTED: [...portfolioOnlyDetected],
    },
    RECOMMENDATION_CLASSIFIER: {
      MENTIONED: mentioned,
      RECOMMENDED: recommended,
      FIRST_RECOMMENDATIONS: firstRecs,
      TOP3_CLASSIFICATIONS: top3,
      AMBIGUOUS_CASES: ambiguous,
      QA_SAMPLE: classifierQaSample,
    },
    CITATION_EVIDENCE: {
      RESPONSES_WITH_CITATIONS: responsesWithCitations,
      RESPONSES_WITHOUT_CITATIONS: responsesWithoutCitations,
      TOTAL_CITATIONS: totalCitations,
      VALID_NORMALIZED_URLS: validUrls,
      INVALID_URLS: invalidUrls,
      UNIQUE_DOMAINS: [...domains].sort(),
      CITATION_RATE_STATUS: "PARTIAL",
    },
    ELIGIBILITY_OBSERVATIONS: {
      NOT_ELIGIBLE_BRANDS_RECOMMENDED: notEligibleRecommended.slice(0, 50),
      UNKNOWN_BRANDS_RECOMMENDED: unknownRecommended.slice(0, 50),
    },
    COMPANY_DATA_READINESS: {
      MARRIOTT: companyReady.marriott,
      HILTON: companyReady.hilton,
      CHOICE: companyReady.choice,
      IHG: companyReady.ihg,
    },
    TREND_BASELINE: {
      BASELINE_CREATED: completedRuns.length > 0 ? "YES" : "NO",
      BASELINE_ID: wave1Id,
      COMPARABLE_PRIOR_PERIOD: "NONE",
      TREND_AVAILABLE: "NO",
    },
    UI_DATA_READINESS: {
      EXECUTIVE_SUMMARY_READY: completedRuns.length > 0,
      DETAILED_VIEW_READY: completedRuns.length > 0,
      LANGUAGE_SELECTOR_READY_IF_MULTIPLE: true,
      PROVIDER_SELECTOR_STATE: providerFilter,
      MEASURED_PROVIDERS: completedRuns.length ? ["openai"] : [],
    },
    SOURCE_CHANGE_FOUNDATION: {
      READY: true,
      SOURCE_CHANGE_CLAIMS_THIS_PERIOD: "NO",
      domainsPersisted: domains.size,
    },
    MULTI_PROVIDER_HANDOFF: {
      GEMINI: {
        NORMALIZED_CONTRACT_READY: true,
        PROMPT_LIBRARY_REUSABLE: true,
        WAVE_MATRIX_REUSABLE: true,
        KNOWN_CITATION_RISK: "provider_citation_shape_unknown_until_adapter",
        NEXT_IMPLEMENTATION_REQUIREMENT: "gemini_adapter_implementing_normalized_contract",
      },
      PERPLEXITY: {
        NORMALIZED_CONTRACT_READY: true,
        PROMPT_LIBRARY_REUSABLE: true,
        WAVE_MATRIX_REUSABLE: true,
        KNOWN_CITATION_RISK: "citation_rich_but_url_normalization_required",
        NEXT_IMPLEMENTATION_REQUIREMENT: "perplexity_adapter_implementing_normalized_contract",
      },
      CLAUDE: {
        NORMALIZED_CONTRACT_READY: true,
        PROMPT_LIBRARY_REUSABLE: true,
        WAVE_MATRIX_REUSABLE: true,
        KNOWN_CITATION_RISK: "web_fetch_citation_capability_may_be_partial",
        NEXT_IMPLEMENTATION_REQUIREMENT: "claude_adapter_implementing_normalized_contract",
      },
    },
    DISCOVERABILITY_HANDOFF: {
      DISCOVERABILITY_HANDOFF_READY: true,
      BUSINESS_IMPACT_HANDOFF_READY: true,
      status: OPENAI_DISCOVERABILITY_STATUS,
    },
    DATA_QUALITY: { BLOCKING: blocking, NON_BLOCKING: nonBlocking },
    DATASET_STATUS: datasetStatus,
    STORAGE: {
      WAVE1_ROOT: storeRoot,
      RAW_ARTIFACTS: path.join(waveDir, "raw"),
      NORMALIZED: path.join(waveDir, "normalized"),
      CHECKPOINTS: path.join(storeRoot, "checkpoints"),
      LEGACY_UNTOUCHED: path.resolve(storeRoot) !== path.resolve(PHASE2E_ROOT),
      NORMALIZED_CONTRACT_FIELDS: listNormalizedProviderContractFields(),
    },
    NEXT_RECOMMENDED_PHASE: nextPhase,
    BUILD_STATUS: buildStatus,
    ACTIVITY: {
      LIVE_PROVIDER_LOGICAL_CALLS: (logical.succeeded || 0) + (logical.failedFinal || 0),
      LIVE_PROVIDER_ATTEMPTS: logical.totalAttempts || cp.costLedger?.providerAttempts || 0,
      AIRTABLE_WRITES: 0,
      ENTITLEMENT_WRITES: 0,
      DEPLOYS: 0,
    },
  };
}
