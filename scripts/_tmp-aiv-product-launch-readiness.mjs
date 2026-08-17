/**
 * Brand AI Visibility — product launch readiness inventory (read-only).
 * CODE_CHANGES product=0 · PROVIDER_CALLS=0 · SCHEDULER_ENABLE=0
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import {
  loadShowcaseCompaniesConfig,
  getShowcasePortfolioBrandIds,
  listShowcaseCompanyKeys,
} from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import {
  buildDiscoverabilityProductPayload,
  resolveOwnedDomainsForBrand,
} from "../lib/ai-visibility/brand-website-wiring.js";
import {
  peerSetBrandNamesById,
  PEER_SET_ID_V2,
} from "../lib/ai-visibility/peer-sets.js";
import {
  buildControlledReleaseMonitoringSop,
  auditSchedulerReadiness,
} from "../lib/ai-visibility/controlled-release-monitoring.js";
import { buildBrandAiVisibilityV1Contract } from "../lib/ai-visibility/signal-architecture/brand-ai-visibility-v1.js";
import { listClientMetricDefinitions } from "../lib/ai-visibility/client-metric-definitions.js";
import { PROVIDER_MODELS } from "../lib/ai-visibility/recurring-period-model.js";
import { getRecurringMonitoringConfig } from "../lib/ai-visibility/recurring-monitoring-config.js";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  getBrandOverviewPayload,
  getBrandQuestionsPayload,
  getBrandSourcesPayload,
  parseGeographyQuery,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload as getExec } from "../lib/ai-visibility/brand-executive-summary.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import {
  canUseDemoBrandPortfolioSwitch,
  demoBrandPortfolioEntitlementOverride,
} from "../lib/dealality/demo-brand-portfolio-context.js";

const outDir = path.join("data", "ai-visibility", "audits");
fs.mkdirSync(outDir, { recursive: true });

const brandNames = peerSetBrandNamesById(PEER_SET_ID_V2);
const showcase = loadShowcaseCompaniesConfig();

function authArgs(brandIds) {
  const graph = buildFixtureEntitlementGraph({
    entitledBrandIds: brandIds,
    peerBrandIds: brandIds,
    source: "demo_showcase_portfolio",
  });
  return {
    dealalityUser: { id: "launch-audit", role: "admin" },
    viewerContext: {
      memberId: "launch-audit",
      roles: ["admin"],
      entitledBrandIds: brandIds,
    },
    entitlementGraph: graph,
    brandNamesById: { ...brandNames },
  };
}

function classifyOwned(domains) {
  const list = domains || [];
  const parent =
    list.find((d) =>
      /^(marriott\.com|ihgplc\.com|hilton\.com|accor\.com|hyatt\.com)$/i.test(d)
    ) || null;
  const development =
    list.find((d) => /hotel-development|development\./i.test(d)) || null;
  const residences = list.find((d) => /residences/i.test(d)) || null;
  const regional =
    list.find((d) => /regional|latam|emea|apac/i.test(d)) || null;
  const brandSpecific = list.filter(
    (d) =>
      d !== parent &&
      d !== development &&
      d !== residences &&
      d !== regional
  );
  let status = "MISSING";
  if (list.length === 0) status = "MISSING";
  else if (parent && brandSpecific.length) status = "COMPLETE";
  else if (parent || brandSpecific.length) status = "PARTIAL";
  else status = "PARTIAL";
  // Marriott-style full set: parent + brand + development preferred
  if (parent && brandSpecific.length && development) status = "COMPLETE";
  else if (parent && brandSpecific.length) status = "PARTIAL";
  else if (list.length >= 1) status = "PARTIAL";
  return {
    PARENT_COMPANY_DOMAIN: parent,
    BRAND_DOMAINS: brandSpecific,
    DEVELOPMENT_DOMAIN: development,
    RESIDENCES_DOMAIN: residences,
    REGIONAL_DOMAIN: regional,
    ALL_DOMAINS: list,
    STATUS: status,
  };
}

async function main() {
  const store = createBrandAiVisibilityReadStore({});
  const portfolioKeys = [...listShowcaseCompanyKeys(showcase)];
  if (!portfolioKeys.includes("marriott")) portfolioKeys.unshift("marriott");
  if (!portfolioKeys.includes("ihg")) portfolioKeys.push("ihg");

  const disc = {
    TOTAL_BRANDS_WITH_GOVERNED_URLS: 0,
    BASELINE_MEASURED: [],
    CHECK_NOT_RUN: [],
    SOURCE_NOT_CONFIGURED: [],
    CHECK_FAILED: [],
    OTHER: [],
  };
  const ownedByPortfolio = {};
  const portfolioMatrix = {};
  const seen = new Set();

  for (const key of portfolioKeys) {
    let pack;
    try {
      pack = getShowcasePortfolioBrandIds(key, showcase);
    } catch {
      continue;
    }
    if (!pack?.ok) {
      portfolioMatrix[key] = { available: false };
      continue;
    }
    const brands = [];
    const ownedRows = [];
    for (const id of pack.brandIds || []) {
      const name = brandNames[id] || id;
      const { owned } = resolveOwnedDomainsForBrand(id, {
        brandNamesById: brandNames,
      });
      const discPayload = buildDiscoverabilityProductPayload(id, {
        brandNamesById: brandNames,
      });
      const domains = owned.ownedDomainList || [];
      const ownedClass = classifyOwned(domains);
      const discState =
        discPayload.DISCOVERABILITY || discPayload.status || "UNKNOWN";
      const row = {
        id,
        name,
        ownedStatus: owned.OWNED_DOMAIN_STATUS,
        ownedClass,
        discState,
        liveBaseline: !!discPayload.LIVE_BASELINE,
        lastChecked: discPayload.LAST_CHECKED_AT || null,
      };
      brands.push(row);
      ownedRows.push({ name, ...ownedClass, OWNED_DOMAIN_STATUS: owned.OWNED_DOMAIN_STATUS });

      if (seen.has(id)) continue;
      seen.add(id);
      if (owned.OWNED_DOMAIN_STATUS === "CONFIGURED") {
        disc.TOTAL_BRANDS_WITH_GOVERNED_URLS += 1;
      }
      if (discState === "SOURCE_NOT_CONFIGURED") {
        disc.SOURCE_NOT_CONFIGURED.push(name);
      } else if (discState === "CHECK_FAILED") {
        disc.CHECK_FAILED.push(name);
      } else if (
        discState === "BASELINE_MEASURED" ||
        discPayload.LIVE_BASELINE
      ) {
        disc.BASELINE_MEASURED.push(name);
      } else if (
        discState === "CHECK_NOT_RUN" ||
        discState === "BASELINE_NOT_RUN"
      ) {
        disc.CHECK_NOT_RUN.push(name);
      } else {
        disc.OTHER.push({ name, discState });
      }
    }
    ownedByPortfolio[key] = ownedRows;
    portfolioMatrix[key] = {
      available: true,
      brandCount: brands.length,
      brands: brands.map((b) => ({
        name: b.name,
        disc: b.discState,
        owned: b.ownedStatus,
        ownedStatusClass: b.ownedClass.STATUS,
      })),
    };
  }

  // Performance sample sizes (in-process payloads, not HTTP)
  const marriott = getShowcasePortfolioBrandIds("marriott", showcase);
  const sizes = {};
  if (marriott.ok) {
    const auth = authArgs(marriott.brandIds);
    const geo = parseGeographyQuery({ geography: "CALA" });
    const exec = await getExec({
      ...auth,
      store,
      geography: geo,
      language: "en",
      provider: "openai",
    });
    const overview = await getBrandOverviewPayload({
      ...auth,
      store,
      brandId: marriott.brandIds[0],
      geography: geo,
      language: "en",
      provider: "openai",
    });
    const questions = await getBrandQuestionsPayload({
      ...auth,
      store,
      brandId: marriott.brandIds[0],
      geography: geo,
      language: "en",
      provider: "openai",
      limit: 25,
      offset: 0,
    });
    const sources = await getBrandSourcesPayload({
      ...auth,
      store,
      brandId: marriott.brandIds[0],
      geography: geo,
      language: "en",
      provider: "openai",
    });
    sizes.EXEC_API_RESPONSE_SIZE = Buffer.byteLength(JSON.stringify(exec));
    sizes.DETAIL_OVERVIEW_SIZE = Buffer.byteLength(JSON.stringify(overview));
    sizes.QUESTIONS_PAGE_SIZE = Buffer.byteLength(JSON.stringify(questions));
    sizes.SOURCES_SIZE = Buffer.byteLength(JSON.stringify(sources));
    sizes.questionsPaginated = !!(
      questions?.pagination ||
      questions?.page ||
      typeof questions?.total === "number"
    );
    sizes.execKeys = Object.keys(exec || {}).slice(0, 20);
    sizes.hasMonitoringMeta = !!(
      exec?.monitoringMeta ||
      exec?.asOf ||
      exec?.lastUpdated ||
      exec?.period ||
      exec?.currentPosition?.asOf
    );
    sizes.freshnessProbe = {
      execAsOf: exec?.asOf || exec?.lastUpdated || exec?.monitoringWindow || null,
      periodLabel: exec?.periodLabel || exec?.currentPeriod?.label || null,
      providersCompleted:
        exec?.providersCompleted ||
        exec?.currentPosition?.providersCompleted ||
        null,
      partial:
        exec?.partial ||
        exec?.monitoringStatus ||
        exec?.currentPosition?.monitoringStatus ||
        null,
    };
  }

  // Demo override isolation probe (no network)
  const fakeProdUser = {
    id: "prod-user",
    role: "member",
    companyId: "recPRODUCTIONCLIENT",
    companyIds: ["recPRODUCTIONCLIENT"],
    isDemo: false,
    flags: {},
  };
  const fakeDemoUser = {
    id: "demo-user",
    role: "admin",
    isAdmin: true,
    flags: { isAdmin: true },
    canAccessBrandWorkspace: true,
    isBrand: true,
    activeWorkspace: "Brand",
    demoBrandPortfolioKey: "marriott",
  };
  const canProd = canUseDemoBrandPortfolioSwitch(fakeProdUser);
  const canDemo = canUseDemoBrandPortfolioSwitch(fakeDemoUser);
  const demoOverrideProd = demoBrandPortfolioEntitlementOverride(fakeProdUser);
  const demoOverrideDemo = demoBrandPortfolioEntitlementOverride({
    ...fakeDemoUser,
    // header simulation path uses user key when present
  });

  // Provider env (presence only — no calls)
  const envProvider = {
    openai: {
      credential: !!(process.env.OPENAI_API_KEY || "").trim(),
      modelConfigured: !!(
        process.env.AI_VISIBILITY_OPENAI_MODEL ||
        process.env.OPENAI_MODEL ||
        PROVIDER_MODELS?.openai
      ),
      defaultModel: PROVIDER_MODELS?.openai || null,
    },
    gemini: {
      credential: !!(
        process.env.GOOGLE_AI_API_KEY ||
        process.env.GEMINI_API_KEY ||
        ""
      ).trim(),
      modelConfigured: !!(
        process.env.AI_VISIBILITY_GEMINI_MODEL || PROVIDER_MODELS?.gemini
      ),
      defaultModel: PROVIDER_MODELS?.gemini || null,
    },
    perplexity: {
      credential: !!(process.env.PERPLEXITY_API_KEY || "").trim(),
      modelConfigured: !!(
        process.env.AI_VISIBILITY_PERPLEXITY_MODEL ||
        PROVIDER_MODELS?.perplexity
      ),
      defaultModel: PROVIDER_MODELS?.perplexity || null,
    },
    claude: {
      credential: !!(process.env.ANTHROPIC_API_KEY || "").trim(),
      modelConfigured: !!(
        process.env.AI_VISIBILITY_CLAUDE_MODEL || PROVIDER_MODELS?.claude
      ),
      defaultModel: PROVIDER_MODELS?.claude || null,
    },
  };

  let recurringConfig = null;
  try {
    recurringConfig = getRecurringMonitoringConfig({});
  } catch (e) {
    recurringConfig = { error: e.message };
  }

  // Scan client JS/HTML for internal terms in visible string literals (rough)
  const clientJs = fs.readFileSync(
    "public/js/ai-visibility/ai-visibility-brand.js",
    "utf8"
  );
  const clientHtml = fs.readFileSync("public/ai-visibility-brand.html", "utf8");
  const copyHits = [];
  const patterns = [
    /\bdeterministic\b/gi,
    /\bclassifier\b/gi,
    /\bscaffold\b/gi,
    /\bevidenceId\b/gi,
    /\bresponseId\b/gi,
    /\bAirtable\b/gi,
    /\bmetric contract\b/gi,
    /\bprovider adapter\b/gi,
  ];
  for (const re of patterns) {
    const mJs = clientJs.match(re) || [];
    const mHtml = clientHtml.match(re) || [];
    if (mJs.length || mHtml.length) {
      copyHits.push({
        term: re.source,
        jsHits: mJs.length,
        htmlHits: mHtml.length,
      });
    }
  }

  // Help coverage vs required definitions
  const defs = listClientMetricDefinitions().map((d) => d.id);
  const requiredHelp = [
    "AI_PRESENCE",
    "PORTFOLIO_AI_PRESENCE",
    "ALL_PROVIDERS",
    "COMPETITIVE_POSITION",
    "QUESTIONS_MISSING",
    "CITATION_RATE",
    "OWNED_SOURCE_CITATION_RATE",
    "PUBLIC_DISCOVERABILITY",
    "COMPARABLE_TREND",
  ];
  const missingDefs = requiredHelp.filter((id) => !defs.includes(id));
  // Provider disagreement / source mix / frequency — check strings in UI
  const helpExtra = {
    PROVIDER_DISAGREEMENT: /Provider Disagreement|disagreement/i.test(clientJs),
    SOURCE_MIX: /Source Mix/i.test(clientJs) || /Source Mix/i.test(clientHtml),
    SOURCE_CITATION_FREQUENCY:
      /Source Citation Frequency|Citation Frequency/i.test(clientJs) ||
      /Source Citation Frequency/i.test(clientHtml),
  };

  const report = {
    generatedAt: new Date().toISOString(),
    auditOnly: true,
    CERTIFIED_LAYER_FROZEN: "YES",
    contract: buildBrandAiVisibilityV1Contract(),
    sop: buildControlledReleaseMonitoringSop(),
    scheduler: auditSchedulerReadiness({ SCHEDULER_ENABLED: false }),
    recurringConfig,
    discoverability: disc,
    ownedByPortfolio,
    portfolioMatrix,
    envProvider,
    sizes,
    authSafety: {
      productionUserCanSwitch: canProd,
      demoUserCanSwitch: canDemo,
      demoOverrideIgnoredForNonDemoUser: demoOverrideProd == null,
      demoOverrideAppliesForDemoUser: demoOverrideDemo != null,
      PRODUCTION_AUTH_SAFETY:
        canProd === false && demoOverrideProd == null ? "PASS" : "FAIL",
    },
    clientCopyHits: copyHits,
    help: {
      definitionsPresent: defs,
      missingRequired: missingDefs,
      extras: helpExtra,
    },
    PROVIDER_MODELS: PROVIDER_MODELS || null,
  };

  const outPath = path.join(
    outDir,
    `product-launch-readiness-${Date.now()}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        outPath,
        disc: {
          governed: disc.TOTAL_BRANDS_WITH_GOVERNED_URLS,
          measured: disc.BASELINE_MEASURED,
          notRun: disc.CHECK_NOT_RUN,
          notConfigured: disc.SOURCE_NOT_CONFIGURED,
          failed: disc.CHECK_FAILED,
          other: disc.OTHER,
        },
        portfolios: Object.fromEntries(
          Object.entries(portfolioMatrix).map(([k, v]) => [
            k,
            { available: v.available, n: v.brandCount },
          ])
        ),
        sizes,
        auth: report.authSafety,
        envProvider,
        sop: report.sop.CURRENT_MODE,
        scheduler: report.scheduler,
        copyHits,
        help: report.help,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
