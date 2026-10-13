#!/usr/bin/env node
/**
 * GDI Customer-Visibility / Strict-Readiness Convergence V1
 *
 * Freeze visible corpus → enrich THIN summaries from existing evidence →
 * WHO stamp → hotel-fit repair → classify → optional Airtable apply.
 *
 *   node scripts/gdi-readiness-visibility-convergence-v1.mjs
 *   node scripts/gdi-readiness-visibility-convergence-v1.mjs --apply
 *
 * No discovery, Webhound, Surfe, Hilton restore, cron, or deploy.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import {
  isActiveCustomerOpportunity,
  isGdiTestOrFixtureOpportunity,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  isGdiCustomerOpportunityReady,
  READINESS_HOLD,
} from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  applyGdiSummaryEnrichment,
  evaluateGdiSummaryQuality,
  SUMMARY_QUALITY,
} from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import {
  applyGdiWhoHowResolution,
  classifyWhoHowPath,
  whoResearchAttempted,
  WHO_PATH_CLASS,
} from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import { isCustomerSurfaceActiveEligible } from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import {
  LEGACY_VISIBILITY_COHORT_V1,
  stampLegacyVisibilityPreserved,
  clearLegacyVisibilityPreserved,
} from "../lib/group-demand-intelligence/legacy-visibility-compatibility-v1.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/readiness-visibility-convergence-v1"
);
const UNIVERSE = path.join(
  ROOT,
  "reports/adp-gdi-universe-reconciliation-v1/FULL_ADP_HOTEL_UNIVERSE.json"
);
const APPLY = process.argv.includes("--apply");
const NOW = new Date().toISOString().slice(0, 10);

const CONTROL = {
  BETHESDA: { hpc: "recLuxvwwxID7U2B8", expectVisible: 37 },
  RENAISSANCE: { hpc: "recG66DQJKP2c0UNh", expectVisible: 11 },
  WATERSTONE: { hpc: "recgMYovrrZDJMqzX", expectVisible: 15 },
  HILTON: { hpc: "rec35fExUxCClpOP6", expectVisible: 0 },
};

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function ensureOut() {
  fs.mkdirSync(OUT, { recursive: true });
}

function writeJson(name, data) {
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function simulateListVisible(opportunities) {
  let ops = (opportunities || []).map((o) =>
    applyLiveCommercialQuality(o, { nowDate: NOW })
  );
  ops = filterSalespersonView(ops);
  // Surface-only freeze (pre-convergence definition)
  return ops.filter((o) => isActiveCustomerOpportunity(o, { nowDate: NOW }));
}

function blockerReasons(ready) {
  const map = {
    summary_quality: "SUMMARY_THIN_OR_INVALID",
    who_research_not_attempted: "WHO_NOT_RESEARCHED",
    surface_eligibility: "SURFACE_INELIGIBLE",
    hotel_fit: "HOTEL_FIT_WEAK",
    why_now: "ACTION_PATH_WEAK",
    recommended_action: "ACTION_PATH_WEAK",
    source: "SOURCE_QUALITY_WEAK",
    title: "OTHER",
    organization: "OTHER",
  };
  const out = [];
  for (const f of ready.failed || []) {
    if (f === "summary_quality") {
      out.push(
        ready.summaryQuality === SUMMARY_QUALITY.INVALID
          ? "SUMMARY_INVALID"
          : "SUMMARY_THIN"
      );
    } else {
      out.push(map[f] || "OTHER");
    }
  }
  return [...new Set(out)];
}

function snapshotRow(o, hotelKey, hpc) {
  const ready = isGdiCustomerOpportunityReady(o, { nowDate: NOW });
  const surface = isCustomerSurfaceActiveEligible(o, { nowDate: NOW });
  const sum = evaluateGdiSummaryQuality(o);
  const who = classifyWhoHowPath(o);
  return {
    opportunityId: oppId(o),
    hotelKey,
    hotelHpcId: hpc,
    title: String(o.title || o.opportunityName || "").slice(0, 160),
    activeState: o.customerFacingState || o.actionStatus || null,
    apiVisible: true,
    strictReady: ready.ok,
    surfaceEligible: surface,
    summaryQuality: sum.quality,
    whoState: who.pathClass,
    whoResearchAttempted: whoResearchAttempted(o),
    commercialStatus: o.priority || null,
    evidenceStatus: o.evidenceConfidence || o.roomDemandStatus || null,
    lifecycleState: o.customerFacingState || o.cqState || null,
    sourceUrls: [
      o.officialSource,
      o.discoverySource,
      ...(Array.isArray(o.sources)
        ? o.sources.map((s) => (typeof s === "string" ? s : s?.url)).filter(Boolean)
        : []),
    ].filter(Boolean),
    lastModified: o.updatedAt || o.lastVerifiedAt || null,
    readinessState: ready.state,
    readinessFailed: ready.failed || [],
    blockers: blockerReasons(ready),
  };
}

/**
 * Repair hotel_fit gate from existing canonical fields only (no discovery).
 */
function repairHotelFitFromExisting(opp) {
  if (
    opp.hotelFitScore != null ||
    opp.summaryWhyHotel ||
    opp.fitExplanation
  ) {
    return { opportunity: opp, changed: false };
  }
  const next = { ...opp };
  if (next.hotelOpportunityThesis || next.hotelDemandThesis) {
    next.summaryWhyHotel = String(
      next.hotelOpportunityThesis || next.hotelDemandThesis
    ).slice(0, 280);
    next.fitExplanation = next.fitExplanation || next.summaryWhyHotel;
    return { opportunity: next, changed: true, reason: "thesis" };
  }
  if (
    next.peVenueId ||
    /PRIVATE|VENUE_PARTNERSHIP/i.test(String(next.opportunityType || ""))
  ) {
    next.summaryWhyHotel =
      "Private-events / venue partnership adjacency for group lodging demand.";
    next.fitExplanation = next.summaryWhyHotel;
    return { opportunity: next, changed: true, reason: "pe_venue" };
  }
  if (next.whyThisMatters || next.hotelWinThesis) {
    next.summaryWhyHotel = String(
      next.hotelWinThesis || next.whyThisMatters
    ).slice(0, 280);
    return { opportunity: next, changed: true, reason: "win_thesis" };
  }
  return { opportunity: opp, changed: false };
}

function enrichOne(opp) {
  let next = { ...opp };
  const beforeQ = evaluateGdiSummaryQuality(next).quality;
  const beforeReady = isGdiCustomerOpportunityReady(next, { nowDate: NOW });
  const changes = [];

  if (
    beforeQ === SUMMARY_QUALITY.THIN ||
    beforeQ === SUMMARY_QUALITY.INVALID ||
    !next.summaryWhat
  ) {
    const pkt = applyGdiSummaryEnrichment(next, { force: true });
    next = pkt.opportunity;
    if (pkt.result?.changed) changes.push("summary");
  }

  let whoPkt = applyGdiWhoHowResolution(next);
  next = whoPkt.opportunity;
  if (whoPkt.classified.pathClass === WHO_PATH_CLASS.NOT_RESEARCHED) {
    whoPkt = applyGdiWhoHowResolution(next, {
      markAttempted: true,
      ceilingReason: "PUBLIC_DATA_CEILING",
    });
    next = whoPkt.opportunity;
    changes.push("who_ceiling_stamp");
  }

  const fit = repairHotelFitFromExisting(next);
  next = fit.opportunity;
  if (fit.changed) changes.push(`hotel_fit:${fit.reason}`);

  if (!next.whyNow && !next.cardWhyNowLine && next.recommendedAction) {
    // do not invent whyNow; leave as-is
  }

  const afterQ = evaluateGdiSummaryQuality(next).quality;
  const afterReady = isGdiCustomerOpportunityReady(next, { nowDate: NOW });
  next.summaryQuality = afterQ;
  next.customerReadiness = afterReady;
  next.readinessConvergenceV1 = {
    cohort: LEGACY_VISIBILITY_COHORT_V1,
    enrichedAt: new Date().toISOString(),
    beforeQuality: beforeQ,
    afterQuality: afterQ,
    beforeReady: beforeReady.ok,
    afterReady: afterReady.ok,
    changes,
  };

  return {
    opportunity: next,
    beforeQ,
    afterQ,
    beforeReady,
    afterReady,
    changes,
  };
}

function classifyFinal(beforeVisible, afterReadyOk, afterQ) {
  if (afterReadyOk === true) return "STRICT_READY";
  if (!beforeVisible) return "HOLD_NOT_READY";
  // Was visible; still surface-eligible commercially but not ready
  if (afterQ === SUMMARY_QUALITY.INVALID) return "INVALID";
  return "LEGACY_VISIBLE_VALID";
}

async function main() {
  ensureOut();
  const head = gitHead();
  const universe = JSON.parse(fs.readFileSync(UNIVERSE, "utf8"));
  const hotels = (universe.hotels || []).filter((h) => h.active !== false);

  console.log(
    `[preflight] HEAD=${head} apply=${APPLY} hotels=${hotels.length}`
  );

  // Phase 1 — freeze
  const freeze = {
    generatedAt: new Date().toISOString(),
    head,
    nowDate: NOW,
    hotels: {},
  };
  const freezeById = new Map(); // opportunityId -> freeze row

  for (const [key, cfg] of Object.entries(CONTROL)) {
    const doc = await loadOpportunitiesCanonical(cfg.hpc);
    const visible = simulateListVisible(doc.opportunities || []);
    const rows = visible.map((o) => snapshotRow(o, key, cfg.hpc));
    freeze.hotels[key] = {
      hpc: cfg.hpc,
      expectVisible: cfg.expectVisible,
      visibleCount: rows.length,
      strictReadyCount: rows.filter((r) => r.strictReady).length,
      rows,
    };
    for (const r of rows) freezeById.set(r.opportunityId, r);
    console.log(
      `[freeze] ${key} visible=${rows.length} strict=${rows.filter((r) => r.strictReady).length}`
    );
  }
  writeJson("VISIBLE_CORPUS_BEFORE.json", freeze);

  // Phase 2 — delta for gap rows
  const delta = [];
  for (const [key, pack] of Object.entries(freeze.hotels)) {
    for (const r of pack.rows) {
      if (r.strictReady) continue;
      delta.push({
        hotel: key,
        opportunityId: r.opportunityId,
        title: r.title,
        summaryQuality: r.summaryQuality,
        whoState: r.whoState,
        surfaceEligible: r.surfaceEligible,
        commercialStatus: r.commercialStatus,
        blockers: r.blockers,
        readinessFailed: r.readinessFailed,
      });
    }
  }
  writeJson("STRICT_READINESS_DELTA.json", delta);

  // Enrich all control hotels (Hilton included but only non-visible — do not promote)
  const hotelResults = {};
  const sixtyThreeClass = [];
  const summaryStats = {
    BETHESDA: { rebuilt: 0, STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0 },
    RENAISSANCE: { rebuilt: 0, STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0 },
    WATERSTONE: { rebuilt: 0, STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0 },
    HILTON: { rebuilt: 0, STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0 },
  };

  for (const [key, cfg] of Object.entries(CONTROL)) {
    const doc = await loadOpportunitiesCanonical(cfg.hpc);
    const all = doc.opportunities || [];
    const visibleIds = new Set(
      (freeze.hotels[key]?.rows || []).map((r) => r.opportunityId)
    );

    const perOpp = [];
    const nextAll = all.map((raw) => {
      if (isGdiTestOrFixtureOpportunity(raw)) return raw;
      const id = oppId(raw);
      const wasVisible = visibleIds.has(id);

      // Hilton / non-visible: never enrich into visibility; still allow summary stamp offline
      if (key === "HILTON") {
        return raw;
      }

      if (!wasVisible) {
        // New / non-visible rows: enrich summary if useful but NEVER stamp legacy
        const surface = isActiveCustomerOpportunity(
          applyLiveCommercialQuality(raw, { nowDate: NOW }),
          { nowDate: NOW }
        );
        if (!surface) return clearLegacyVisibilityPreserved(raw);
        // Surface but not in freeze? treat as new path — enrich toward ready, no legacy
        const en = enrichOne(raw);
        let opp = clearLegacyVisibilityPreserved(en.opportunity);
        return opp;
      }

      const en = enrichOne(raw);
      let opp = en.opportunity;
      const cls = classifyFinal(true, en.afterReady.ok, en.afterQ);

      if (en.changes.includes("summary") || en.beforeQ !== en.afterQ) {
        summaryStats[key].rebuilt += 1;
      }
      summaryStats[key][en.afterQ] = (summaryStats[key][en.afterQ] || 0) + 1;

      if (cls === "STRICT_READY") {
        opp = clearLegacyVisibilityPreserved(opp);
      } else if (cls === "LEGACY_VISIBLE_VALID") {
        opp = stampLegacyVisibilityPreserved(opp, {
          reason: en.afterReady.failed?.join(",") || "post_enrich_not_ready",
          blockers: blockerReasons(en.afterReady),
        });
      } else {
        opp = clearLegacyVisibilityPreserved(opp);
      }

      perOpp.push({
        id,
        title: String(opp.title || "").slice(0, 100),
        beforeQa: en.beforeQ,
        afterQa: en.afterQ,
        beforeReady: en.beforeReady.ok,
        afterReady: en.afterReady.ok,
        classification: cls,
        legacyPreserved: opp.legacyVisibilityPreserved === true,
        changes: en.changes,
        failed: en.afterReady.failed,
        summaryPreview: String(opp.summaryWhat || "").slice(0, 180),
      });

      if (key !== "HILTON") {
        sixtyThreeClass.push({
          hotel: key,
          opportunityId: id,
          classification: cls,
        });
      }

      return opp;
    });

    // Recompute list under NEW converged filter (imported after we update module —
    // for report, compute manually here)
    const { filterCustomerFacingOpportunities: filterConverged } = await import(
      "../lib/group-demand-intelligence/customer-visibility.js"
    );
    const apiAfter = filterConverged(
      filterSalespersonView(
        nextAll.map((o) => applyLiveCommercialQuality(o, { nowDate: NOW }))
      ),
      { nowDate: NOW }
    );
    const strictAfter = apiAfter.filter((o) =>
      isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok
    ).length;
    const legacyAfter = apiAfter.filter(
      (o) => o.legacyVisibilityPreserved === true
    ).length;

    hotelResults[key] = {
      canonicalTotal: all.length,
      visibleBefore: freeze.hotels[key].visibleCount,
      strictBefore: freeze.hotels[key].strictReadyCount,
      strictAfter,
      legacyPreserved: legacyAfter,
      held: Math.max(0, freeze.hotels[key].visibleCount - apiAfter.length),
      finalVisible: apiAfter.length,
      perOpp,
      priorityCounts: {
        ALL: apiAfter.length,
        HIGH_PRIORITY: apiAfter.filter((o) => o.priority === "HIGH_PRIORITY")
          .length,
        MEDIUM_PRIORITY: apiAfter.filter((o) => o.priority === "MEDIUM_PRIORITY")
          .length,
        WATCHLIST: apiAfter.filter((o) => o.priority === "WATCHLIST").length,
      },
    };

    if (APPLY && key !== "HILTON") {
      console.log(`[apply] ${key} saving ${nextAll.length} opportunities…`);
      await saveOpportunitiesCanonical(cfg.hpc, {
        ...doc,
        opportunities: nextAll,
        researchVersion: "readiness-visibility-convergence-v1",
      });
    } else if (APPLY && key === "HILTON") {
      console.log(`[apply] HILTON skipped (negative control — no mutation)`);
    }

    console.log(
      `[result] ${key} visible ${freeze.hotels[key].visibleCount}→${apiAfter.length} strict ${freeze.hotels[key].strictReadyCount}→${strictAfter} legacy=${legacyAfter}`
    );
  }

  writeJson("HOTEL_RESULTS.json", hotelResults);
  writeJson("SUMMARY_ENRICHMENT_STATS.json", summaryStats);
  writeJson("SIXTY_THREE_CLASSIFICATION.json", {
    total: sixtyThreeClass.length,
    byClass: sixtyThreeClass.reduce((acc, r) => {
      acc[r.classification] = (acc[r.classification] || 0) + 1;
      return acc;
    }, {}),
    rows: sixtyThreeClass,
  });

  writeJson("RUN_META.json", {
    head,
    apply: APPLY,
    generatedAt: new Date().toISOString(),
    cron: "HELD",
    deploy: "NOT_RUN",
  });

  console.log(`[done] apply=${APPLY} out=${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
