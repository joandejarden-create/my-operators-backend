/**
 * GDI Yield Recovery V1.1 — Revalidate + controlled promote of lost V1.2 TRUEs.
 *
 *   node scripts/gdi-yield-recovery-v1-1-promote-lost-trues.mjs
 *   node scripts/gdi-yield-recovery-v1-1-promote-lost-trues.mjs --apply
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  promoteQualifiedGdiOpportunity,
  PROMOTION_ACTION,
  NEWNESS_SEMANTICS,
} from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import { invalidateGdiHotelReadCache } from "../lib/group-demand-intelligence/read-cache.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/yield-recovery-v1-1"
);
const APPLY = process.argv.includes("--apply");
const HOTEL = "recLuxvwwxID7U2B8";
const DISCOVERY_RUN_ID = "gdi_new_opps_v1_2_live_bethesda_20260923";
const DISCOVERY_AT = "2026-09-23T23:36:33.455Z";
const PROMOTE_RUN_ID = `gdi_yield_recovery_v1_1_${new Date()
  .toISOString()
  .slice(0, 10)
  .replace(/-/g, "")}`;

async function headCheck(url) {
  try {
    const r = await fetch(url, {
      redirect: "follow",
      signal: AbortSignal.timeout(20000),
      headers: { "User-Agent": "DealalityGDI-YieldRecovery/1.1" },
    });
    const text = await r.text();
    return {
      ok: r.status >= 200 && r.status < 400,
      status: r.status,
      finalUrl: r.url,
      hasHotel: /hotel|lodging|accommodat|housing|room.?block|travel/i.test(text),
      hasFuture: /2026|2027|2028/i.test(text),
    };
  } catch (err) {
    return { ok: false, error: err.message };
  }
}

function slugify(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 48);
}

function buildCandidateFromV12(row, hotelId) {
  const start = String(row.timing || "").slice(0, 10);
  const demandType = String(row.demandType || "");
  let opportunityType = "PRIMARY_PURSUIT";
  if (/overflow/i.test(demandType)) opportunityType = "OVERFLOW_HOUSING";
  else if (/sport/i.test(demandType)) opportunityType = "OVERFLOW_HOUSING";
  else if (/training/i.test(demandType)) opportunityType = "PRIMARY_PURSUIT";
  else if (/event|association/i.test(demandType)) opportunityType = "PRIMARY_PURSUIT";

  const id = `gdi_opp_${slugify(row.title)}_${start.replace(/-/g, "").slice(0, 8)}`;
  return {
    id,
    opportunityId: id,
    hotelId,
    title: row.title,
    opportunityName: row.title,
    organizationName: row.organization,
    organizationId: slugify(row.organization),
    opportunityType,
    opportunityTypeLabel: opportunityType.replace(/_/g, " "),
    demandType: row.demandType,
    segment: row.demandType,
    priority: "HIGH_PRIORITY",
    opportunityQualification: "STRONG",
    eventStartDate: start,
    eventLocationStatus: "VERIFIED_CITY",
    eventLocationSummary: row.location,
    venueSourcingStatus: "PRIMARY_VENUE_SELECTED_OVERFLOW_POSSIBLE",
    sourcingStatus: "OPEN_UNRESOLVED",
    roomDemandStatus: "VERIFIED_HOUSING_PROGRAM",
    lodgingEvidence: row.housingEvidence,
    housingStatus: row.housingStatus,
    officialSource: row.officialSource,
    discoverySource: row.officialSource,
    sources: [
      {
        name: row.title,
        url: row.officialSource,
        sourceType: "official_web",
        date: DISCOVERY_AT.slice(0, 10),
        supportsFact: "housing",
        claimKind: "FACT",
      },
    ],
    whyNow: `Official housing/travel evidence confirmed for ${start}; lodging demand path open.`,
    summaryWhyMatters: row.housingEvidence,
    summaryWhat: `${row.title} — ${row.demandType} with official lodging evidence.`,
    recommendedAction: row.suggestedAction,
    hotelFitScore: 70,
    evidenceConfidence: 0.85,
    isTestData: false,
    customerVisible: true,
    demandStatus: "ACTIVE",
    notes: row.notes || [],
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const v12 = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "reports/group-demand-intelligence/gdi-new-opportunities-v1-2-live-bethesda-audit/LIVE_REQUALIFY_FINAL.json"
      ),
      "utf8"
    )
  );
  const liveTrue = v12.liveNewTrue || [];
  const doc = await loadOpportunitiesCanonical(HOTEL);
  const existing = doc.opportunities || [];

  const results = [];

  for (const row of liveTrue) {
    const check = await headCheck(row.officialSource);
    const isPremier = /premier cup/i.test(row.title || "");
    const candidate = buildCandidateFromV12(row, HOTEL);
    // Prefer live final hotel URL when redirected
    if (check.finalUrl && check.finalUrl !== row.officialSource) {
      candidate.officialSource = check.finalUrl;
      candidate.discoverySource = check.finalUrl;
      candidate.sources[0].url = check.finalUrl;
      candidate.sources.push({
        name: "Prior discovery URL",
        url: row.officialSource,
        sourceType: "official_web",
        date: DISCOVERY_AT.slice(0, 10),
        supportsFact: "source",
        claimKind: "FACT",
      });
    }

    const stillFuture = (() => {
      const d = new Date(candidate.eventStartDate);
      const t = new Date();
      t.setHours(0, 0, 0, 0);
      return Number.isFinite(d.getTime()) && d >= t;
    })();

    let recommendation = PROMOTION_ACTION.PROMOTE_NEW;
    let forceUpdateId = null;
    let materialUpdateOnly = false;

    if (!check.ok || !check.hasHotel) {
      recommendation = PROMOTION_ACTION.REJECT_STALE;
    } else if (!stillFuture) {
      recommendation = PROMOTION_ACTION.REJECT_STALE;
    } else if (isPremier) {
      forceUpdateId = "gdi_opp_bethesda_premier_cup_2026";
      materialUpdateOnly = true;
      recommendation = PROMOTION_ACTION.UPDATE_EXISTING;
      candidate.id = forceUpdateId;
      candidate.opportunityId = forceUpdateId;
    }

    const revalidation = {
      candidate: row.title,
      organization: row.organization,
      demandType: row.demandType,
      timing: row.timing,
      officialSource: candidate.officialSource,
      lodgingEvidence: row.housingEvidence,
      venueSourcing: candidate.venueSourcingStatus,
      hotelFit: candidate.hotelFitScore,
      actionPath: candidate.recommendedAction,
      sourceCheck: check,
      stillFuture,
      currentQualification: stillFuture && check.ok && check.hasHotel ? "STRONG" : "REJECT",
      recommendation,
    };

    if (recommendation === PROMOTION_ACTION.REJECT_STALE) {
      results.push({
        ...revalidation,
        promotion: { action: recommendation, dryRun: !APPLY },
      });
      continue;
    }

    const promotion = await promoteQualifiedGdiOpportunity({
      candidate,
      existingOpps: existing,
      hotelId: HOTEL,
      runId: PROMOTE_RUN_ID,
      discoveryRunId: DISCOVERY_RUN_ID,
      discoveryAt: DISCOVERY_AT,
      method: "new_opportunities_v1_2_controlled_promote",
      playbook: isPremier ? "SPORTS_HOUSING" : /overflow/i.test(row.demandType)
        ? "LODGING_HOUSING"
        : /training/i.test(row.demandType)
          ? "TRAINING_PROGRAM"
          : "EVENT_FUTURE_CYCLE",
      source: candidate.officialSource,
      dryRun: !APPLY,
      forceUpdateId,
      materialUpdateOnly,
    });

    results.push({ ...revalidation, promotion });
    if (APPLY && promotion.action === PROMOTION_ACTION.PROMOTE_NEW) {
      existing.push(promotion.opportunity);
    }
  }

  invalidateGdiHotelReadCache(HOTEL);
  const after = APPLY
    ? await loadOpportunitiesCanonical(HOTEL)
    : doc;
  const customer = filterCustomerFacingOpportunities(after.opportunities || []);
  const neu = customer.filter(
    (o) => o.weeklyDeltaState === "NEW" || o.isNewThisWeek === true
  );

  const summary = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    promoteRunId: PROMOTE_RUN_ID,
    discoveryRunId: DISCOVERY_RUN_ID,
    newnessSemantics: NEWNESS_SEMANTICS.NEW_TO_GDI_ON_PROMOTION,
    newnessPolicyNote:
      "Customer NEW = first time in canonical GDI (promotion). firstDiscovered* preserved from V1.2 dry-run discovery run. Not falsely labeled as discovered today.",
    results,
    counts: {
      promoteNew: results.filter(
        (r) => r.promotion?.action === PROMOTION_ACTION.PROMOTE_NEW
      ).length,
      updateExisting: results.filter(
        (r) => r.promotion?.action === PROMOTION_ACTION.UPDATE_EXISTING
      ).length,
      held: results.filter(
        (r) => r.promotion?.action === PROMOTION_ACTION.HOLD_WATCH
      ).length,
      rejected: results.filter(
        (r) => r.promotion?.action === PROMOTION_ACTION.REJECT_STALE
      ).length,
      duplicate: results.filter(
        (r) => r.promotion?.action === PROMOTION_ACTION.DUPLICATE_EXISTING
      ).length,
    },
    visibility: {
      canonicalTotal: (after.opportunities || []).length,
      customerVisible: customer.length,
      weeklyNew: neu.length,
      newIds: neu.map((o) => o.id),
    },
  };

  const outPath = path.join(OUT, `PROMOTE_${APPLY ? "APPLY" : "DRY"}_${Date.now()}.json`);
  fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        apply: APPLY,
        counts: summary.counts,
        weeklyNew: summary.visibility.weeklyNew,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
