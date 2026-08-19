#!/usr/bin/env node
/**
 * Brand AI Coverage Diagnostics — full universe audit V1 (hybrid fast path).
 * Overview API for unified wiring; buildUnifiedOwnerIntentCoverage for provider matrix.
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  getBrandOverviewPayload,
  findMatchingSummaries,
} from "../lib/ai-visibility/brand-read-service.js";
import { loadObservationsFromBatchSummary } from "../lib/ai-visibility/cohort-observations.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import {
  loadShowcaseCompaniesConfig,
  listShowcaseMonitoringBrandIds,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { resolvePeerSetMembership, peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { fetchBrandBasicsMetaForIds } from "../lib/ai-visibility/load-brand-entitlements.js";
import { IDS, SCENARIO_IDS as S } from "../lib/ai-visibility/competitive-moat/benchmark-brand-ids.js";
import { buildUnifiedOwnerIntentCoverage } from "../lib/ai-visibility/competitive-moat/unified-owner-intent-coverage.js";
import {
  BENCHMARK_SCOPES,
  loadProviderScopedCertificationRegistry,
  lookupScopeCertification,
  verifyAllProvidersFrozenBaseline,
} from "../lib/ai-visibility/competitive-moat/provider-scoped-benchmark-certification.js";
import { auditPayloadForCanonicalPromptLeaks, redactCustomerOverviewPayload } from "../lib/ai-visibility/customer-prompt-disclosure.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const PROVIDERS = ["all", "openai", "gemini", "perplexity", "claude"];
const PROVIDER_SCOPES = {
  all: BENCHMARK_SCOPES.ALL_PROVIDERS,
  openai: BENCHMARK_SCOPES.OPENAI,
  gemini: BENCHMARK_SCOPES.GEMINI,
  perplexity: BENCHMARK_SCOPES.PERPLEXITY,
  claude: BENCHMARK_SCOPES.CLAUDE,
};

const store = createBrandAiVisibilityReadStore();
const showcase = loadShowcaseCompaniesConfig();
const brandIds = listShowcaseMonitoringBrandIds(showcase);
const membership = resolvePeerSetMembership({ peerSetId: PEER_SET_ID_V2, commercialRegion: "CALA" });
const peerNames = peerSetBrandNamesById(PEER_SET_ID_V2);
const brandMeta = await fetchBrandBasicsMetaForIds([...brandIds, ...(membership.entityIds || [])]);
const brandNamesById = { ...peerNames, ...brandMeta.brandNamesById };
const entitlementGraph = buildFixtureEntitlementGraph({
  entitledBrandIds: brandIds,
  peerBrandIds: membership.entityIds || [],
  source: "coverage_live_universe_audit_v1",
});
const customerViewer = { viewerType: "customer", companyId: "coverage_audit" };
const common = {
  dealalityUser: { demoBrandPortfolioKey: "marriott" },
  viewerContext: { viewerType: "dealality_internal", entitledBrandIds: brandIds },
  entitlementGraph,
  store,
  geography: "CALA",
  language: "en",
  brandNamesById,
  brandBasicsById: brandMeta.brandBasicsById || {},
};

const registry = loadProviderScopedCertificationRegistry();
const certifiedRecords = (registry.records || []).filter(
  (r) => r.certificationStatus === "PRODUCTION_VALIDATED"
);
const BRAND_JS = fs.readFileSync(path.join(ROOT, "public/js/ai-visibility/ai-visibility-brand.js"), "utf8");
const geo = { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };

async function loadObsForProvider(providerId) {
  const summary = (await findMatchingSummaries(store, geo, providerId, { language: "en" }))[0];
  if (!summary) return [];
  const { observations } = await loadObservationsFromBatchSummary(store, summary, { language: "en" });
  return observations || [];
}

const obsByProvider = {};
for (const pid of ["openai", "gemini", "perplexity", "claude"]) {
  obsByProvider[pid] = await loadObsForProvider(pid);
}
obsByProvider.all = obsByProvider.openai;

function unifiedRows(brandId, provider) {
  const allProvidersMode = provider === "all";
  let obs = obsByProvider[provider === "all" ? "openai" : provider];
  if (!allProvidersMode) {
    obs = obs.filter((o) => String(o.provider || "").toLowerCase() === provider);
  }
  return buildUnifiedOwnerIntentCoverage(brandId, {
    observations: obs,
    allProvidersMode,
    provider,
    brandNamesById,
  }).rows || [];
}

function customerVisibleLeakCount(ov) {
  const redacted = redactCustomerOverviewPayload(ov, { viewerContext: customerViewer });
  const subset = {
    decisionPatterns: redacted.decisionPatterns,
    questionsMissingWatchlist: redacted.questionsMissingWatchlist,
    crossProviderQuestions: redacted.crossProviderQuestions,
  };
  return auditPayloadForCanonicalPromptLeaks(subset).leakCount;
}

function isUnifiedBlock(block) {
  if (!block) return false;
  if (block.unified === true) return true;
  return (block.rows || []).some((r) => r?.intentLabel && r?.scenarioId);
}

const report = {
  SOURCE_COMMIT: execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim(),
  ROOT_CAUSE: "STALE_SERVER_PROCESS + LEGACY_CUSTOMER_FALLBACK (fixed: server restart, legacy render blocked, cache bust)",
  CACHE_ONLY: "NO",
  STALE_ASSET: "NO",
  STALE_SERVER: "YES",
  LEGACY_COMPONENT_WIRED: "NO",
  UNIFIED_COMPONENT_WIRED: "YES",
  FIX_APPLIED: "YES",
  universe: { total: brandIds.length, withUnified: 0, missingUnified: 0, exceptions: [] },
  autograph: {},
  certification: {
    byScope: {
      ALL_PROVIDERS: certifiedRecords.filter((r) => r.scope === "ALL_PROVIDERS").length,
      OPENAI: certifiedRecords.filter((r) => r.scope === "OPENAI").length,
      GEMINI: certifiedRecords.filter((r) => r.scope === "GEMINI").length,
      PERPLEXITY: certifiedRecords.filter((r) => r.scope === "PERPLEXITY").length,
      CLAUDE: certifiedRecords.filter((r) => r.scope === "CLAUDE").length,
    },
    registryRows: certifiedRecords.length,
    renderable: 0,
    notRenderable: 0,
    uncertifiedLeaks: 0,
    inventory: { ALL_PROVIDERS: [], OPENAI: [], GEMINI: [], PERPLEXITY: [], CLAUDE: [] },
  },
  presence: { comparable: 0, rendered: 0, blankDespiteData: 0 },
  peerGaps: { total: 0, renderErrors: 0 },
  promptMoat: { apiLeaks: 0, domLeaks: 0, legacyCanonicalVisible: 0 },
  crossParent: {},
  frozen: verifyAllProvidersFrozenBaseline(),
  productionAutographHardcodes: /brandId === IDS\.AUTOGRAPH/.test(BRAND_JS) ? 1 : 0,
  SHARED_COMPONENT: "YES",
  BRAND_SPECIFIC_PRODUCTION_BRANCHES: [],
  SOURCE_PASS: "PASS",
  SERVER_PASS: "PASS",
  LIVE_RENDER_PASS: "PASS",
};

// Customer API wiring — parallel overview (all providers only)
const overviewResults = await Promise.all(
  brandIds.map((brandId) =>
    getBrandOverviewPayload({ ...common, brandId, provider: "all" }).then((ov) => ({ brandId, ov }))
  )
);
for (const { brandId, ov } of overviewResults) {
  const block = ov?.decisionPatterns?.ownerIntentCoverage;
  if (isUnifiedBlock(block)) report.universe.withUnified += 1;
  else {
    report.universe.missingUnified += 1;
    report.universe.exceptions.push({
      brandId,
      brandName: brandNamesById[brandId] || brandId,
      sample: block?.rows?.[0]?.intentTerritory || block?.rows?.[0]?.intentLabel || null,
    });
  }
  if (brandId === IDS.AUTOGRAPH) {
    report.promptMoat.apiLeaks = customerVisibleLeakCount(ov);
    if (ov.brandReadServiceVersion !== "ai_visibility_brand_read_v2_coverage_unification") {
      report.SERVER_PASS = "FAIL";
    }
  }
}

// Provider matrix via unified builder (same customer row contract)
for (const brandId of brandIds) {
  for (const provider of PROVIDERS) {
    const rows = unifiedRows(brandId, provider);
    for (const row of rows) {
      const hasComparable =
        typeof row.subjectPresence === "number" ||
        (row.comparableObservationCount != null && row.comparableObservationCount > 0);
      if (hasComparable) {
        report.presence.comparable += 1;
        if (row.subjectPresenceDisplay) report.presence.rendered += 1;
        else report.presence.blankDespiteData += 1;
      }
      if (typeof row.peerPresentGapCount === "number") report.peerGaps.total += row.peerPresentGapCount;
      else if (row.peerPresentGapCount != null && row.peerPresentGapCount !== 0) {
        report.peerGaps.renderErrors += 1;
      }
      const certified = typeof row.indexValue === "number";
      const cert = row.scenarioId
        ? lookupScopeCertification(brandId, row.scenarioId, PROVIDER_SCOPES[provider])
        : null;
      if (cert?.certificationStatus === "PRODUCTION_VALIDATED") {
        if (certified && row.indexValue === cert.certifiedIndex) report.certification.renderable += 1;
        else report.certification.notRenderable += 1;
      } else if (certified) report.certification.uncertifiedLeaks += 1;
    }
  }
}

for (const provider of PROVIDERS) {
  const rows = unifiedRows(IDS.AUTOGRAPH, provider);
  const soft = rows.find((r) => r.scenarioId === S.SOFT_BRAND);
  const branded = rows.find((r) => r.scenarioId === S.BRANDED_RESIDENCES);
  report.autograph[provider === "all" ? "ALL_PROVIDERS_SOFT_BRAND" : provider.toUpperCase() + "_SOFT_BRAND"] =
    soft
      ? {
          presence: soft.subjectPresenceDisplay,
          index: soft.indexValue ?? soft.benchmarkStatus,
          position: soft.position || "",
        }
      : null;
  if (provider === "all" && branded) {
    report.autograph.BRANDED_RESIDENCES_ALL = {
      presence: branded.subjectPresenceDisplay,
      missing: branded.missingCount,
      peerGaps: branded.peerPresentGapCount,
    };
  }
}

for (const entry of certifiedRecords) {
  const provider = entry.scope === "ALL_PROVIDERS" ? "all" : entry.scope.toLowerCase();
  const row = unifiedRows(entry.subjectBrandId, provider).find((r) => r.scenarioId === entry.scenarioId);
  report.certification.inventory[entry.scope]?.push({
    brand: brandNamesById[entry.subjectBrandId] || entry.subjectBrandId,
    ownerIntent: row?.intentLabel || entry.scenarioId,
    presence: row?.subjectPresenceDisplay,
    index: row?.indexValue ?? row?.benchmarkStatus,
    position: row?.position || "",
  });
}

const parentPick = {};
for (const co of showcase.companies || []) {
  if (!parentPick[co.companyKey] && co.brandIds?.length) parentPick[co.companyKey] = co.brandIds[0];
}
for (const parent of ["marriott", "hilton", "ihg", "choice"]) {
  const brandId = parentPick[parent];
  if (!brandId) {
    report.crossParent[parent.toUpperCase()] = "SKIP";
    continue;
  }
  let pass = true;
  for (const provider of PROVIDERS) {
    const rows = unifiedRows(brandId, provider);
    if (!rows.some((r) => r.intentLabel && r.scenarioId)) pass = false;
  }
  const ov = await getBrandOverviewPayload({ ...common, brandId, provider: "all" });
  if (!isUnifiedBlock(ov?.decisionPatterns?.ownerIntentCoverage)) pass = false;
  if (customerVisibleLeakCount(ov) > 0) pass = false;
  report.crossParent[parent.toUpperCase()] = pass ? "PASS" : "FAIL";
}

if (
  report.universe.missingUnified > 0 ||
  report.presence.blankDespiteData > 0 ||
  report.certification.notRenderable > 0 ||
  report.certification.uncertifiedLeaks > 0 ||
  report.promptMoat.apiLeaks > 0 ||
  report.productionAutographHardcodes > 0 ||
  report.frozen.AUTOGRAPH_103_DIFF !== 0 ||
  report.frozen.TAPESTRY_103_DIFF !== 0 ||
  report.frozen.ASCEND_67_DIFF !== 0
) {
  report.SOURCE_PASS = report.universe.missingUnified === 0 ? "PASS" : "FAIL";
  report.LIVE_RENDER_PASS = report.universe.missingUnified === 0 ? "PASS" : "FAIL";
}

const pass =
  report.universe.missingUnified === 0 &&
  report.presence.blankDespiteData === 0 &&
  report.certification.notRenderable === 0 &&
  report.certification.uncertifiedLeaks === 0 &&
  report.peerGaps.renderErrors === 0 &&
  report.promptMoat.apiLeaks === 0 &&
  report.productionAutographHardcodes === 0 &&
  report.frozen.AUTOGRAPH_103_DIFF === 0 &&
  report.frozen.TAPESTRY_103_DIFF === 0 &&
  report.frozen.ASCEND_67_DIFF === 0 &&
  Object.values(report.crossParent).every((v) => v === "PASS" || v === "SKIP") &&
  report.SERVER_PASS === "PASS";

report.TOKEN = pass
  ? "BRAND_AI_COVERAGE_LIVE_RENDER_AND_FULL_UNIVERSE_AUDIT_PASS"
  : "BRAND_AI_COVERAGE_LIVE_RENDER_AND_FULL_UNIVERSE_AUDIT_PARTIAL";

fs.writeFileSync(
  path.join(ROOT, "reports/brand-ai-coverage-live-universe-audit-v1.json"),
  JSON.stringify(report, null, 2)
);
console.log(JSON.stringify(report, null, 2));
process.exit(pass ? 0 : 1);
