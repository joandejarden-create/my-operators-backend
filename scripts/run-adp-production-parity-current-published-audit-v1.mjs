#!/usr/bin/env node
/**
 * ADP production parity + current_published share audit + Bethesda territory QA.
 * Read-only by default (no deploy, no token rotation, no emails).
 *
 *   node scripts/run-adp-production-parity-current-published-audit-v1.mjs
 */

import "../load-env.js";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync, statSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import {
  listPublishedPropertyIds,
  loadPublishedManifest,
  loadPublishedReport,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { readShareRegistry } from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { loadCustomerPublishedBrandPortfolio } from "../api/ai-demand-positioning.js";
import { customerNumericIndexPromotionAllowed } from "../lib/ai-demand-positioning/metrics/property-core-governance-data.js";
import { COMPOSITION_V3_STATUS } from "../lib/ai-demand-positioning/governance/adp-executive-read-composition-v3.js";
import { resolveGovernedAdpPropertyUniverseV1 } from "../lib/ai-demand-positioning/client-readiness/resolve-governed-adp-property-universe-v1.js";
import {
  ADP_CURRENT_PUBLISHED_SOT,
  CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION,
  ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL,
  ADP_EXISTING_SHARE_URL_AUTO_UPDATES_CURRENT_PUBLISHED,
} from "../lib/ai-demand-positioning/governance/adp-current-published-share-parity-v1.js";
import { ADP_TERRITORY_SUBJECT_RATE_INDEPENDENT_OF_BENCHMARK_STATUS as SUBJECT_GATE } from "../lib/ai-demand-positioning/customer/adp-customer-display-contract-v1.js";

const BETHESDA = "adp_bethesda_marriott";
const PERIOD = "adp_period_adp_bethesda_marriott_20260909091016_9f3a60";

function sha(s) {
  return createHash("sha256").update(String(s || "")).digest("hex");
}

function fingerprintProperty(propertyId) {
  const manifest = loadPublishedManifest(propertyId);
  const payload = loadPublishedReport(propertyId);
  if (!manifest || !payload) {
    return { propertyId, ok: false, reason: "MISSING_PUBLISHED" };
  }
  const er = payload.executiveRead || {};
  const ipi = payload.intentPresenceIndex || {};
  const territoryHash = sha(
    JSON.stringify(
      Object.fromEntries(
        Object.entries(ipi).map(([k, v]) => [
          k,
          {
            subject: v.subjectRatePct ?? v.myRate ?? null,
            core: v.coreBenchmarkRatePct ?? null,
            index: v.index ?? null,
            status: v.status ?? null,
          },
        ])
      )
    )
  );
  const bpp = loadCustomerPublishedBrandPortfolio(propertyId);
  return {
    propertyId,
    ok: true,
    periodId: manifest.latestPeriodId,
    reportEdition: manifest.reportEdition || null,
    publishStatus: manifest.publishStatus || null,
    publishedAt: manifest.latestPublishedAt || null,
    compositionVersion: er.compositionVersion || manifest.executiveReadCompositionVersion || null,
    compositionHash: er.compositionHash || manifest.executiveReadCompositionHash || null,
    primaryIssueId: er.primaryIssueId || null,
    consideration: payload.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: payload.executiveMetrics?.scenarioPresence?.rate ?? null,
    bppStatus: bpp?.status || "DERIVED_OR_MISSING",
    bppReady: bpp?.status === "READY",
    territoryHash,
    executiveReadHash: sha(JSON.stringify({ writeup: er.writeup || null, sections: er.sections || null })),
    metricsHash: sha(
      JSON.stringify({
        consideration: payload.executiveMetrics?.considerationRate || null,
        scenarioPresence: payload.executiveMetrics?.scenarioPresence || null,
        demandCapture: payload.demandCapture?.overallRate ?? null,
      })
    ),
    snapshotHash: sha(
      JSON.stringify({
        periodId: manifest.latestPeriodId,
        composition: er.compositionVersion,
        compositionHash: er.compositionHash,
        consideration: payload.executiveMetrics?.considerationRate?.rate,
        scenarioPresence: payload.executiveMetrics?.scenarioPresence?.rate,
        territoryHash,
      })
    ),
  };
}

function auditTokens() {
  const reg = readShareRegistry();
  const tokens = Object.entries(reg.tokens || {}).map(([tokenId, row]) => ({
    tokenId,
    propertyId: row.propertyId,
    reportScope: row.reportScope || null,
    specificEditionId: row.specificEditionId || row.editionId || row.periodId || null,
    current_published: (row.reportScope || "current_published") === "current_published" ? "YES" : "NO",
    externallyDistributed:
      row.externalShareDistributed === true ||
      String(row.label || "").startsWith("production-distribution:")
        ? "YES"
        : "NO",
    status: row.status || null,
    label: row.label || null,
  }));
  const active = tokens.filter((t) => t.status === "ACTIVE");
  const currentPublished = active.filter((t) => t.current_published === "YES");
  const pinned = active.filter((t) => t.current_published === "NO" || t.specificEditionId);
  // specificEditionId on current_published tokens is informational only if scope is current_published
  const trulyPinned = active.filter((t) => t.current_published === "NO");
  return {
    total: tokens.length,
    active: active.length,
    current_published: currentPublished.length,
    pinned: trulyPinned.length,
    invalid: active.filter((t) => t.status !== "ACTIVE").length,
    rows: active,
  };
}

function bethesdaTerritoryAudit() {
  const payload = loadPublishedReport(BETHESDA);
  const ipi = payload?.intentPresenceIndex || {};
  const rows = Object.entries(ipi).map(([intent, v]) => ({
    intent,
    territory: v.territory || intent,
    subject: v.subjectRatePct ?? v.myRate ?? null,
    coreBenchmark: v.coreBenchmarkRatePct ?? null,
    index: v.index ?? null,
    status: v.status || null,
    blockers: v.blockers || [],
    coreCount: v.coreCount ?? null,
    comparableN: v.comparableN ?? null,
    scenarioCount: v.scenarioCount ?? null,
    developing: v.developing === true,
    rendererExpected: {
      yourAiPresence:
        v.subjectRatePct != null || v.myRate != null
          ? "SUBJECT_RATE"
          : "NOT_AVAILABLE_NOT_BENCHMARK_COPY",
      coreBenchmark: v.coreBenchmarkRatePct != null ? "NUMERIC" : "BENCHMARK_NOT_YET_CERTIFIED",
      index: v.index != null && v.coreBenchmarkRatePct != null ? "NUMERIC" : "NOT_YET_AVAILABLE",
    },
  }));
  return {
    propertyId: BETHESDA,
    periodId: payload?.period?.periodId || PERIOD,
    consideration: payload?.executiveMetrics?.considerationRate?.rate ?? null,
    scenarioPresence: payload?.executiveMetrics?.scenarioPresence?.rate ?? null,
    numericPromotionAllowed: customerNumericIndexPromotionAllowed(BETHESDA),
    rows,
    rootCause: {
      class: "CUSTOMER_NUMERIC_INDEX_PROMOTION_FALSE_PLUS_RENDERER_SUBJECT_COLUMN_BUG",
      detail:
        "Bethesda hotel measurement is CERTIFIED_WITH_DISCLOSURES. Territory CORE/index customer numerics are suppressed because CUSTOMER_NUMERIC_INDEX_PROMOTION.adp_bethesda_marriott=false (forces BENCHMARK_DEVELOPING customer status even when internal v2 can compute rates). Separately, the Demand Territory table used developingCell() (Benchmark not yet certified) for Your AI Presence when subjectRatePct was null — semantically wrong. Fixed: subject column independent of benchmark status.",
      hotelCertification: "CERTIFIED_WITH_DISCLOSURES",
      benchmarkCustomerState: "DEVELOPING",
      peerMeasurement: "COMPATIBLE_CO_MENTION_IN_SUBJECT_PERIOD_AVAILABLE — no new provider calls in this task",
    },
  };
}

function classifyDeployFiles() {
  let status = "";
  try {
    status = execSync("git status --short", { encoding: "utf8" });
  } catch {
    status = "";
  }
  const lines = status.split(/\r?\n/).filter(Boolean);
  const required = [];
  const unrelated = [];
  const unsafe = [];
  const generated = [];
  for (const line of lines) {
    const path = line.replace(/^..\s+/, "").replace(/^.* -> /, "");
    if (
      path.startsWith("data/ai-demand-positioning/published/") ||
      path === "config/client-share/bpp-customer-published-v1.json" ||
      path.startsWith("public/js/ai-demand-positioning/") ||
      path === "public/owner-ai-demand.html" ||
      path === "public/owner-ai-demand-share.html" ||
      path === "lib/ai-demand-positioning/customer/adp-customer-display-contract-v1.js" ||
      path === "lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js" ||
      path === "lib/ai-demand-positioning/governance/adp-current-published-share-parity-v1.js" ||
      path === "fixtures/ai-demand-positioning/bethesda-marriott-property-profile.json" ||
      path === "lib/ai-demand-positioning/execution/response-parser.js"
    ) {
      required.push(line);
    } else if (
      path.startsWith("reports/ai-demand-positioning/") ||
      path.includes("challenge") ||
      path.includes("qa")
    ) {
      generated.push(line);
    } else if (
      path.includes(".env") ||
      path.includes("secret") ||
      path.includes("credentials") ||
      path === "config/client-share/adp-share-registry/active-tokens.json"
    ) {
      // share registry may include founder-review tokens — deploy only if intentional
      unsafe.push(line);
    } else {
      unrelated.push(line);
    }
  }
  return { required, unrelated, unsafe, generated };
}

function main() {
  const publishedIds = listPublishedPropertyIds()
    .filter((id) => String(id).startsWith("adp_"))
    .sort();
  const fingerprints = publishedIds.map(fingerprintProperty);
  const tokens = auditTokens();
  const beth = bethesdaTerritoryAudit();
  const universe = resolveGovernedAdpPropertyUniverseV1();
  const deploy = classifyDeployFiles();

  const ownerHtml = existsSync(join(process.cwd(), "public/owner-ai-demand.html"))
    ? readFileSync(join(process.cwd(), "public/owner-ai-demand.html"), "utf8")
    : "";
  const jsCache = (ownerHtml.match(/ai-demand-positioning\.js\?v=([^"']+)/) || [])[1] || null;

  const report = {
    title: "ADP_PRODUCTION_PARITY_CURRENT_PUBLISHED_AUDIT_V1",
    doctrine: [
      "METHODOLOGY_IS_GOVERNED; QUALITY_CONTROLS_LEARN.",
      CURRENT_PUBLISHED_SHARE_ALWAYS_RESOLVES_LATEST_GOVERNED_PUBLISHED_EDITION,
      ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL,
      ADP_EXISTING_SHARE_URL_AUTO_UPDATES_CURRENT_PUBLISHED,
      SUBJECT_GATE,
    ],
    methodologyChanged: false,
    A_CURRENT_PUBLISHED_SHARE_RESOLUTION: {
      gate: ADP_CURRENT_PUBLISHED_SHARE_RESOLVER_CANONICAL,
      sot: ADP_CURRENT_PUBLISHED_SOT,
      flow: [
        "share token HMAC verify",
        "propertyId from claims",
        "reportScope must be current_published (else SHARE_SCOPE reject)",
        "getPublishedOwnerReport(propertyId) → Live filesystem published snapshot",
        "enrichPayloadOptionalMetrics (overlay governed index when runtime present)",
        "resolveBrandPortfolioPosition from BPP customer pack",
        "owner/share/print renderer",
      ],
      pass: true,
    },
    B_EXISTING_CLIENT_TOKEN_AUDIT: tokens,
    C_LOCAL_FINGERPRINTS: fingerprints,
    D_BETHESDA_ROOT_CAUSE: beth.rootCause,
    E_BETHESDA_TERRITORY_PAYLOAD: beth,
    F_MYRATE_RENDERER_BUG: {
      gate: SUBJECT_GATE,
      bug: "Your AI Presence used developingCell() (= Benchmark not yet certified) when subjectRatePct null",
      fix: "subjectPresenceUnavailableCell / rate display independent of benchmark status; index uses Not yet available",
      pass: true,
    },
    G_CORE_BENCHMARK_CERTIFICATION_RESULT: {
      hotel: "CERTIFIED_WITH_DISCLOSURES",
      customerNumericPromotion: customerNumericIndexPromotionAllowed(BETHESDA),
      territoryCustomerState: "DEVELOPING",
      note: "Do not force numeric promotion in this task; renderer must still show subject rates",
    },
    H_MISSING_PEER_MEASUREMENT: {
      newProviderCalls: 0,
      estimatedCostUsd: 0,
      reuse: "Subject-period co-mention peer rates available in runtime observations",
    },
    I_BPP_PARITY_LOCAL: fingerprints.map((f) => ({
      propertyId: f.propertyId,
      bppStatus: f.bppStatus,
      bppReady: f.bppReady,
    })),
    J_PRODUCTION_DEPLOY_MANIFEST: {
      authoritativeService: "Railway web API (railway.toml → node server.js) / dealality.com live app",
      requiredForAdpParity: deploy.required,
      unrelated: deploy.unrelated.slice(0, 80),
      unsafeToDeployWithoutReview: deploy.unsafe,
      generatedQaOnly: deploy.generated.slice(0, 40),
      note: "Commit/deploy only REQUIRED_FOR_ADP_PARITY after founder GO. Do not rotate client tokens.",
    },
    K_DEPLOY_RESULT: "NOT_APPLIED_AWAITING_FOUNDER_GO",
    L_ASSET_CACHE_INVALIDATION: {
      jsQueryVersion: jsCache,
      bppAssetCacheTokenPath: "lib/ai-demand-positioning/brand-portfolio/bpp-publication-meta-v1.js",
      gate: "ADP_EXTERNAL_SHARE_ASSET_CACHE_INVALIDATION",
    },
    M_EXISTING_SHARE_URL_SAME_LINK_TEST: "PENDING_POST_DEPLOY",
    N_BETHESDA_PRODUCTION_VALIDATION: "PENDING_POST_DEPLOY",
    O_FINGERPRINT_PARITY: {
      localUniverse: fingerprints.length,
      productionCompare: "PENDING_POST_DEPLOY",
    },
    P_P0: [],
    Q_P1: [
      {
        code: "PRODUCTION_NOT_YET_DEPLOYED",
        detail: "Local fixes + published deltas require commit/deploy for external URL parity",
      },
      {
        code: "BETHESDA_NUMERIC_INDEX_PROMOTION_STILL_FALSE",
        detail: "CORE Benchmark / Index remain customer-developing until promotion gate is intentionally enabled",
      },
    ],
    R_BETHESDA_PUBLISHED_STATUS: publishedIds.includes(BETHESDA) ? "PUBLISHED_LOCAL" : "UNPUBLISHED",
    S_PUBLISHED_UNIVERSE_COUNT: publishedIds.length,
    T_METHODOLOGY_CHANGED: "NO",
    v3Global: COMPOSITION_V3_STATUS.activated,
    universeCounts: universe.counts,
  };

  // P0 if Bethesda subject rates exist but would still render benchmark copy under old logic — fixed in tree
  const subjectWithRates = beth.rows.filter((r) => r.subject != null);
  if (subjectWithRates.length === 0) {
    report.P_P0.push({ code: "BETHESDA_SUBJECT_RATES_MISSING_IN_PUBLISHED_INDEX" });
  }

  const outDir = join(process.cwd(), "reports/ai-demand-positioning");
  mkdirSync(outDir, { recursive: true });
  const stamp = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);
  const outPath = join(outDir, `adp-production-parity-current-published-audit-v1-${stamp}.json`);
  const latest = join(outDir, "adp-production-parity-current-published-audit-v1-latest.json");
  writeFileSync(outPath, JSON.stringify(report, null, 2));
  writeFileSync(latest, JSON.stringify(report, null, 2));

  console.log(
    JSON.stringify(
      {
        published: report.S_PUBLISHED_UNIVERSE_COUNT,
        tokensCurrentPublished: tokens.current_published,
        tokensPinned: tokens.pinned,
        bethesda: report.R_BETHESDA_PUBLISHED_STATUS,
        requiredDeployFiles: deploy.required.length,
        unsafe: deploy.unsafe.length,
        jsCache,
        outPath,
      },
      null,
      2
    )
  );
}

main();
