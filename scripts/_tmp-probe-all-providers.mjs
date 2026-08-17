/**
 * Probe All Providers payloads for Executive + Detailed View.
 * Read-only — no Airtable writes.
 */
import "dotenv/config";
import { createBrandAiVisibilityReadStore } from "../lib/ai-visibility/storage/index.js";
import {
  getBrandPortfolioPayload,
  getBrandOverviewPayload,
  getBrandTrendPayload,
  getBrandCompetitorsPayload,
  getBrandSourcesPayload,
} from "../lib/ai-visibility/brand-read-service.js";
import { getBrandExecutiveSummaryPayload } from "../lib/ai-visibility/brand-executive-summary.js";
import { buildFixtureEntitlementGraph } from "../lib/ai-visibility/entitlements.js";
import { resolvePeerSetMembership, peerSetBrandNamesById, PEER_SET_ID_V2 } from "../lib/ai-visibility/peer-sets.js";
import { loadShowcaseCompaniesConfig } from "../lib/ai-visibility/brand-ai-showcase-companies.js";
import { fetchBrandBasicsMetaForIds } from "../lib/ai-visibility/load-brand-entitlements.js";

const store = createBrandAiVisibilityReadStore({
  rootDir: process.env.AI_VISIBILITY_STORE_ROOT || null,
});

const showcase = loadShowcaseCompaniesConfig();
const marriott = (showcase.companies || []).find((c) => c.companyKey === "marriott");
const brandIds = marriott?.brandIds || [];
const brandNamesById = Object.fromEntries(
  (marriott?.brands || []).map((b) => [b.brandId, b.brandName])
);
const membership = resolvePeerSetMembership({
  peerSetId: PEER_SET_ID_V2,
  commercialRegion: "CALA",
});
const peerNames = peerSetBrandNamesById(PEER_SET_ID_V2);
const brandMeta = await fetchBrandBasicsMetaForIds([
  ...brandIds,
  ...(membership.entityIds || []),
]);

const entitlementGraph = buildFixtureEntitlementGraph({
  entitledBrandIds: brandIds,
  peerBrandIds: membership.entityIds || [],
  source: "probe_all_providers",
});

const names = {
  ...peerNames,
  ...brandMeta.brandNamesById,
  ...brandNamesById,
};
const basics = brandMeta.brandBasicsById || {};

const viewer = {
  viewerCompanyId: "probe",
  accessDepthDefault: "deep",
};

const common = {
  dealalityUser: { demoBrandPortfolioKey: "marriott" },
  viewerContext: {
    viewerType: "dealality_internal",
    companyId: null,
    entitledBrandIds: brandIds,
  },
  entitlementGraph,
  store,
  geography: "CALA",
  language: "en",
  brandNamesById: names,
  brandBasicsById: basics,
  provider: "all",
};

function kpiSnap(label, metric) {
  if (!metric) return { label, missing: true };
  return {
    label,
    availability: metric.availability || null,
    display: metric.display || null,
    value: metric.value ?? metric.rank ?? null,
  };
}

console.log("=== PORTFOLIO (All Providers) ===");
const portfolio = await getBrandPortfolioPayload(common);
console.log({
  ok: portfolio.ok,
  ALL_PROVIDERS_DERIVED: portfolio.ALL_PROVIDERS_DERIVED,
  brandCount: portfolio.brands?.length,
  brands: (portfolio.brands || []).map((b) => ({
    name: b.brandName,
    presenceAvail: b.aiPresence?.availability,
    presenceDisplay: b.aiPresence?.display,
    rankDisplay: b.competitivePosition?.display,
    rankAvail: b.competitivePosition?.availability,
  })),
});

console.log("\n=== EXECUTIVE SUMMARY (All Providers) ===");
const exec = await getBrandExecutiveSummaryPayload(common);
const cp = exec.currentPosition || {};
console.log({
  ok: exec.ok,
  ALL_PROVIDERS_DERIVED: exec.ALL_PROVIDERS_DERIVED,
  providerMode: exec.providerMode,
  portfolioAiPresence: kpiSnap("portfolio", cp.portfolioAiPresence),
  brandsMonitored: cp.brandsMonitored,
  topBrand: cp.topBrandByAiPresence,
  bestRank: cp.bestCompetitivePosition,
  questionsMissing: kpiSnap("missing", cp.questionsMissing),
  geoSummary: (exec.geographySummary || []).map((g) => ({
    g: g.geography,
    avail: g.availability,
    monitored: g.displayMonitored,
    lead: g.topBrandByAiPresence?.brandName,
    leadPresence: g.topBrandByAiPresence?.display,
    best: g.bestCompetitivePosition?.display,
  })),
  allProvidersPanel: {
    READY: exec.allProvidersPanel?.READY,
    NOT_COMPARABLE: exec.allProvidersPanel?.NOT_COMPARABLE,
    avg: exec.allProvidersPanel?.CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE,
    monitored: exec.allProvidersPanel?.PROVIDERS_MONITORED,
    message: exec.allProvidersPanel?.message,
  },
  languageComparison: exec.languageComparison
    ? {
        status: exec.languageComparison.status,
        note: exec.languageComparison.presenceNote,
        en: exec.languageComparison.EN_AI_PRESENCE_RATE,
        es: exec.languageComparison.ES_AI_PRESENCE_RATE,
      }
    : null,
  competitiveContext: {
    status: exec.competitiveContext?.status,
    message: exec.competitiveContext?.message,
    leader: exec.competitiveContext?.leadingPeer?.name,
  },
  evidenceSummary: {
    status: exec.evidenceSummary?.status,
    message: exec.evidenceSummary?.message,
  },
});

const tributeId = "recCvV0PuZOi8c3hC";
console.log("\n=== BRAND OVERVIEW Tribute (All Providers) ===");
const overview = await getBrandOverviewPayload({
  ...common,
  brandId: tributeId,
});
console.log({
  ok: overview.ok,
  availability: overview.availability,
  availabilityMessage: overview.availabilityMessage,
  provider: overview.provider,
  kpis: Object.fromEntries(
    Object.entries(overview.kpis || {}).map(([k, v]) => [
      k,
      { availability: v?.availability, display: v?.display, value: v?.value ?? v?.rank },
    ])
  ),
  secondary: Object.fromEntries(
    Object.entries(overview.secondary || {}).map(([k, v]) => [
      k,
      { availability: v?.availability, display: v?.display },
    ])
  ),
  owned: overview.ownedDomainResolution?.OWNED_DOMAIN_STATUS,
  ownedDomains: overview.ownedDomainResolution?.ownedDomainList,
  providerPresence: overview.providerPresencePanel
    ? {
        rows: (overview.providerPresencePanel.rows || []).map((r) => ({
          provider: r.provider,
          status: r.MONITORING_STATUS_DISPLAY || r.MONITORING_STATUS,
          presence: r.PRESENCE_RATE_DISPLAY || r.PRESENCE_RATE,
        })),
      }
    : null,
  reviewItems: (overview.reviewItems || []).length,
});

console.log("\n=== TREND / COMPETITORS / SOURCES Tribute (All Providers) ===");
const [trend, comps, sources] = await Promise.all([
  getBrandTrendPayload({ ...common, brandId: tributeId }),
  getBrandCompetitorsPayload({ ...common, brandId: tributeId }),
  getBrandSourcesPayload({ ...common, brandId: tributeId }),
]);
console.log({
  trend: {
    ok: trend.ok,
    availability: trend.availability,
    points: (trend.points || []).length,
    message: trend.message || trend.availabilityMessage,
  },
  competitors: {
    ok: comps.ok,
    availability: comps.availability,
    count: (comps.competitors || []).length,
    subject: (comps.competitors || []).find((c) => c.isSubject),
  },
  sources: {
    ok: sources.ok,
    availability: sources.availability,
    count: (sources.sources || sources.citedSourceIntelligence?.topCitedSources || []).length,
    message: sources.message || sources.availabilityMessage,
  },
});
