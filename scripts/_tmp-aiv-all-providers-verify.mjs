#!/usr/bin/env node
/**
 * Bounded revalidation for All Providers + client-state remediation.
 */
import fs from "fs";
import path from "path";
import {
  createBrandAiVisibilityReadStore,
} from "../lib/ai-visibility/storage/index.js";
import {
  getBrandOverviewPayload,
  parseGeographyQuery,
  resolveBrandGeographyMonitoringState,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import {
  loadObservationsByProviderForCohort,
  computeBrandCrossProviderQuestionsMissing,
} from "../lib/ai-visibility/cross-provider-questions.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { findMatchingSummaries } from "../lib/ai-visibility/brand-read-service.js";
import { normalizeLanguage } from "../lib/ai-visibility/language-dimension.js";

const AUTOGRAPH = "recEJCTDj1zrsjPM6";
const TRIBUTE = "recCvV0PuZOi8c3hC";
const AC = "rec9aZp7GHtzUEg0c";
const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];

function authArgs(brandIds) {
  return {
    dealalityUser: { id: "test-admin", role: "admin" },
    viewerContext: {
      memberId: "test",
      roles: ["admin"],
      entitledBrandIds: brandIds,
    },
    entitlementGraph: {
      entitledBrandIds: brandIds,
      peerBrandIds: brandIds,
    },
  };
}

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const report = {
    generatedAt: new Date().toISOString(),
    allProviders: {},
    providerSpecific: {},
    isolation: { SPANISH_IN_ENGLISH: 0, ENGLISH_IN_SPANISH: 0, PROVIDER_CROSS_CONTAMINATION: 0 },
    clientSource: {},
  };

  // Portfolio All Providers
  const exec = await getBrandExecutiveSummaryPayload({
    store,
    provider: "all",
    geography: "CALA",
    language: "en",
    brandNamesById: {
      [AUTOGRAPH]: "Autograph",
      [TRIBUTE]: "Tribute",
      [AC]: "AC Hotels",
    },
    ...authArgs([AUTOGRAPH, TRIBUTE, AC]),
  });
  report.allProviders.portfolio = {
    OPENAI_SCAFFOLD_REMOVED: exec.OPENAI_SCAFFOLD_REMOVED_FOR_QM === true,
    qmAggregation: exec.currentPosition?.questionsMissing?.aggregation,
    qmValue: exec.currentPosition?.questionsMissing?.value,
    presenceAggregation: exec.currentPosition?.portfolioAiPresence?.aggregation,
    presenceHelper: exec.currentPosition?.portfolioAiPresence?.helper,
  };

  for (const [brandId, name, geo, lang] of [
    [AUTOGRAPH, "Autograph", "CALA", "en"],
    [TRIBUTE, "Tribute", "CALA", "es"],
    [AC, "AC Hotels", "Europe", "en"],
  ]) {
    const overview = await getBrandOverviewPayload({
      store,
      brandId,
      provider: "all",
      geography: geo,
      language: lang,
      brandNamesById: { [brandId]: name },
      ...authArgs([brandId]),
    });
    report.allProviders[`${name}|${geo}|${lang}`] = {
      ok: overview.ok !== false,
      OPENAI_SCAFFOLD_REMOVED: overview.OPENAI_SCAFFOLD_REMOVED_FOR_QM === true,
      qm: overview.kpis?.questionsMissing?.value ?? null,
      denom: overview.kpis?.questionsMissing?.denominator ?? null,
      disagreement:
        overview.crossProviderQuestions?.PROVIDER_DISAGREEMENT_N ?? null,
      presence: overview.kpis?.aiPresence?.value ?? null,
    };
  }

  for (const provider of PROVIDERS) {
    const mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId: AUTOGRAPH,
      geoFilter: parseGeographyQuery({ geography: "CALA" }),
      provider,
      language: "en",
    });
    const arith =
      typeof mon.promptDenominator === "number" &&
      typeof mon.questionsPresent === "number" &&
      typeof mon.questionsMissing === "number" &&
      mon.questionsPresent + mon.questionsMissing === mon.promptDenominator;
    report.providerSpecific[provider] = {
      monitored: mon.promptDenominator,
      present: mon.questionsPresent,
      missing: mon.questionsMissing,
      presence: mon.presenceVal,
      arithmeticPass: arith,
      slotObs: mon.slotObservationCount,
    };
  }

  // Language isolation under all providers load
  const byProviderEn = await loadObservationsByProviderForCohort({
    store,
    geoFilter: parseGeographyQuery({ geography: "CALA" }),
    language: "en",
  });
  for (const pack of Object.values(byProviderEn)) {
    for (const o of pack.observations || []) {
      if (normalizeLanguage(o.language) === "es") report.isolation.SPANISH_IN_ENGLISH += 1;
    }
  }
  const byProviderEs = await loadObservationsByProviderForCohort({
    store,
    geoFilter: parseGeographyQuery({ geography: "CALA" }),
    language: "es",
  });
  for (const pack of Object.values(byProviderEs)) {
    for (const o of pack.observations || []) {
      if (normalizeLanguage(o.language) === "en") report.isolation.ENGLISH_IN_SPANISH += 1;
    }
  }

  // Provider contamination: openai load should only be openai
  const summaries = await findMatchingSummaries(
    store,
    parseGeographyQuery({ geography: "CALA" }),
    "openai",
    { language: "en" }
  );
  if (summaries[0]) {
    const { observations } = await loadObservationsFromBatchSummary(store, summaries[0], {
      matchedSlotKeys: summaries[0]._matchedSlotKeys,
      language: "en",
    });
    for (const o of observations) {
      const p = String(o.provider?.name || o.provider || "").toLowerCase();
      if (p && p !== "openai") report.isolation.PROVIDER_CROSS_CONTAMINATION += 1;
    }
  }

  const clientSrc = fs.readFileSync(
    "public/js/ai-visibility/ai-visibility-brand.js",
    "utf8"
  );
  report.clientSource = {
    REQUEST_GENERATION_GUARD: clientSrc.includes("beginLoadGeneration"),
    ABORT_CONTROLLER: clientSrc.includes("AbortController"),
    CONTEXT_MATCH_BEFORE_APPLY: clientSrc.includes("shouldApplyLoadResult"),
  };

  const brandQm = computeBrandCrossProviderQuestionsMissing({
    byProvider: byProviderEn,
    subjectBrandId: AUTOGRAPH,
  });
  report.brandCrossProviderSample = {
    denominator: brandQm.denominator,
    missingAcrossAll: brandQm.MISSING_ACROSS_ALL_N,
    disagreement: brandQm.PROVIDER_DISAGREEMENT_N,
    presentAcrossAll: brandQm.PRESENT_ACROSS_ALL_N,
  };

  const outDir = path.join("data", "ai-visibility", "audits");
  fs.mkdirSync(outDir, { recursive: true });
  const outPath = path.join(
    outDir,
    `all-providers-client-state-${Date.now()}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ outPath, report }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
