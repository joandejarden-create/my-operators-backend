#!/usr/bin/env node
/**
 * Hilton Times Square — GDI Commercial Depth V2
 *
 * Deepens all existing bag opportunities (lodging + contact + drawer + Jev).
 * Does not start broad discovery first.
 *
 *   node scripts/gdi-hilton-ts-commercial-depth-v2.mjs --dry-run
 *   node scripts/gdi-hilton-ts-commercial-depth-v2.mjs --apply
 *   node scripts/gdi-hilton-ts-commercial-depth-v2.mjs --apply --limit=3
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical, saveOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { promoteQualifiedGdiOpportunity } from "../lib/group-demand-intelligence/promote-qualified-opportunity.js";
import {
  deepenOpportunityCommercialDepthV2,
  classifyLodgingPrimaryMotion,
  COMMERCIAL_DEPTH_V2,
} from "../lib/group-demand-intelligence/commercial-depth-v2.js";
import { classifyContactTier } from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";
import { gradeContactCompleteness } from "../lib/group-demand-intelligence/contact-completeness-v1.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildGenericParityScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/generic-profile-scenarios.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const HOTEL_ID = "rec35fExUxCClpOP6";
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT = path.join(
  path.resolve(__dirname, ".."),
  "reports/group-demand-intelligence/hotel4-hilton-commercial-depth-v2"
);

const APPLY = process.argv.includes("--apply");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : null;

function countBy(arr, fn) {
  const m = {};
  for (const x of arr) {
    const k = fn(x) || "UNKNOWN";
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

function coverage(rows, fn) {
  const n = rows.filter(fn).length;
  return { n, pct: rows.length ? Math.round((1000 * n) / rows.length) / 10 : 0 };
}

function fieldCoverage(rows) {
  const has = (v) => v != null && String(v).trim() !== "" && !/^UNKNOWN$/i.test(String(v));
  return {
    dates: coverage(rows, (o) => has(o.eventStartDate) || has(o.eventYear)),
    venueStatus: coverage(rows, (o) => has(o.venueSourcingStatus) && o.venueSourcingStatus !== "UNKNOWN"),
    roomDemand: coverage(rows, (o) => has(o.roomDemandStatus) && o.roomDemandStatus !== "UNKNOWN"),
    attendance: coverage(rows, (o) => o.attendance != null || o.attendanceStatus === "CONFIRMED"),
    peakRooms: coverage(rows, (o) => o.peakRooms != null || o.peakRoomsStatus === "CONFIRMED"),
    historicalHousing: coverage(rows, (o) => has(o.historicalHousing) || o.lodgingEvidence?.hostHotelMentioned),
    thesis: coverage(rows, (o) => has(o.hotelOpportunityThesis) && String(o.hotelOpportunityThesis).length > 60),
    whyHotel: coverage(rows, (o) => has(o.summaryWhyHotel) && /times square|478|midtown/i.test(o.summaryWhyHotel)),
    whyNow: coverage(rows, (o) => has(o.whyNow) && !/^future cycle 20\d{2}\.?$/i.test(o.whyNow)),
    contact: coverage(rows, (o) => /NAMED_|FUNCTIONAL|ORGANIZATION/.test(classifyContactTier(o))),
    source: coverage(rows, (o) => (o.sources || []).length > 0 || has(o.officialSource)),
    action: coverage(rows, (o) => has(o.recommendedAction) && String(o.recommendedAction).length > 40),
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const hotelConfig = loadHotelDemandConfig(HOTEL_ID);
  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  let opps = [...(doc.opportunities || [])];
  const beforeCf = filterCustomerFacingOpportunities(opps);
  const beforeCoverage = fieldCoverage(beforeCf);
  const beforeContacts = countBy(beforeCf, (o) => classifyContactTier(o));

  // Research ALL bag rows (16), including DQ-hidden — customer-visible is subset
  let targets = opps;
  if (LIMIT && Number.isFinite(LIMIT)) targets = targets.slice(0, LIMIT);

  console.error(`[depth-v2] hotel=${HOTEL_ID} targets=${targets.length} apply=${APPLY}`);

  const results = [];
  const jev = {
    calls: 0,
    safeApply: 0,
    same: 0,
    helpfulDifferent: 0,
    wrong: 0,
    highConfWrong: 0,
    unknown: 0,
    contactUpgrades: 0,
    actionabilityUpgrades: 0,
    playbookApplies: 0,
  };

  for (const opp of targets) {
    console.error(`[depth-v2] researching ${opp.id} — ${opp.title}`);
    const row = await deepenOpportunityCommercialDepthV2(opp, {
      hotelConfig,
      enableSafeApply: true,
      allowSerp: true,
      maxPages: 4,
    });
    results.push({
      id: opp.id,
      title: opp.title,
      motion: row.motion,
      contactBefore: row.contactBefore,
      contactAfter: row.contactAfter,
      gradeBefore: row.gradeBefore,
      gradeAfter: row.gradeAfter,
      actionable: row.actionableGate,
      lodging: {
        housingPage: row.lodging.signals.housingPageFound,
        roomBlock: row.lodging.signals.roomBlockMentioned,
        overflow: row.lodging.signals.overflowMentioned,
        attendance: row.lodging.signals.attendance,
        peakRooms: row.lodging.signals.peakRooms,
        housingOpen: row.lodging.signals.housingOpen,
      },
      playbook: row.lodging.playbook,
      contactJev: row.contact.jev
        ? {
            calls: row.contact.jev.calls,
            safeApply: row.contact.jev.safeApplyCount,
            quality: row.contact.jevQuality?.tallies || null,
          }
        : null,
      person: row.after.primaryContact?.name || row.after.primaryContactName || null,
      role: row.after.primaryContact?.role || null,
      whyNow: row.after.whyNow,
      action: row.after.recommendedAction,
      customerFacingState: row.after.customerFacingState,
      priority: row.after.priority,
    });

    // Jev tallies
    const pb = row.lodging.playbook;
    if (pb) {
      jev.calls += 1;
      if (pb.applied) {
        jev.safeApply += 1;
        jev.playbookApplies += 1;
        jev.helpfulDifferent += 1;
      } else if (pb.agreement) jev.same += 1;
      else jev.unknown += 1;
    }
    const cj = row.contact.jev;
    if (cj) {
      jev.calls += cj.calls || 0;
      jev.safeApply += cj.safeApplyCount || 0;
      const q = row.contact.jevQuality?.tallies || {};
      jev.same += q.SAME || 0;
      jev.helpfulDifferent += q.HELPFUL_DIFFERENT || 0;
      jev.wrong += q.WRONG || 0;
      jev.highConfWrong += q.HIGH_CONFIDENCE_WRONG || q.HIGH_CONF_WRONG || 0;
      jev.unknown += q.UNKNOWN || 0;
    }
    if (row.gradeAfter < row.gradeBefore || (row.gradeBefore > row.gradeAfter === false && row.contactAfter !== row.contactBefore)) {
      const order = { E: 0, D: 1, C: 2, B: 3, A: 4 };
      if ((order[row.gradeAfter] ?? 0) > (order[row.gradeBefore] ?? 0)) jev.contactUpgrades += 1;
    }
    if (row.actionableGate?.actionable && opp.priority === "WATCHLIST") {
      jev.actionabilityUpgrades += 1;
    }

    // Replace in working set
    opps = opps.map((o) => (o.id === opp.id ? row.after : o));
  }

  if (APPLY) {
    for (const r of results) {
      const after = opps.find((o) => o.id === r.id);
      await promoteQualifiedGdiOpportunity({
        candidate: after,
        existingOpps: opps,
        hotelId: HOTEL_ID,
        runId: `hilton_ts_commercial_depth_v2_${new Date().toISOString().slice(0, 10).replace(/-/g, "")}`,
        method: COMMERCIAL_DEPTH_V2,
        forceUpdateId: r.id,
        materialUpdateOnly: true,
        dryRun: false,
      });
    }
    await saveOpportunitiesCanonical(HOTEL_ID, {
      ...doc,
      opportunities: opps,
      commercialDepthV2: {
        at: new Date().toISOString(),
        version: COMMERCIAL_DEPTH_V2,
        researched: results.length,
      },
      updatedAt: new Date().toISOString(),
    });
  }

  const afterDoc = APPLY ? await loadOpportunitiesCanonical(HOTEL_ID) : { opportunities: opps };
  const afterCf = filterCustomerFacingOpportunities(afterDoc.opportunities || []);
  const afterCoverage = fieldCoverage(afterCf);
  const afterContacts = countBy(afterCf, (o) => classifyContactTier(o));

  const movement = countBy(results, (r) => {
    if (r.actionable?.actionable) return "WATCH_TO_ACTIONABLE";
    if (r.customerFacingState === "HOUSING") return "WATCH_TO_HOUSING";
    if (r.customerFacingState === "OVERFLOW") return "WATCH_TO_OVERFLOW";
    if (r.customerFacingState === "FUTURE_WATCH") return "WATCH_TO_FUTURE_WATCH";
    return "UNCHANGED_OR_WATCH";
  });

  // ADP parity audit
  const profile = loadPropertyProfile("adp_hilton_times_square");
  const universe = buildScenarioUniverse(profile);
  const parity = buildGenericParityScenarioUniverse(profile);
  const bySource = countBy(universe, (s) => s.source);
  const adpParity = {
    CURRENT_LIVE_BASELINE: 50,
    CURRENT_AFTER_FIX: universe.length,
    bySource,
    PARITY_TARGET: parity.meta || { total: parity.scenarios?.length },
    ROOT_CAUSE:
      "Market pack nyc_times_square returned 50 standard scenarios; property-capability layer was not merged because combined.length>0 short-circuited generic parity.",
    CLASSIFICATION: "MARKET_LAYER_PRESENT_BUT_CAPABILITY_LAYER_THIN",
    DEFECT: true,
    FIX: "buildScenarioUniverse now merges generateGenericPropertyCapabilityScenarios onto market packs",
    RERUN_REQUIRED: true,
    ESTIMATED_COST_USD: Number((((universe.length * 4) / 200) * 6.5).toFixed(2)),
    NEW_SCENARIO_COUNT: universe.length,
  };

  const namedContacts = results
    .filter((r) => /NAMED_/.test(r.contactAfter) && r.person)
    .map((r) => ({
      opportunity: r.title,
      person: r.person,
      role: r.role,
      grade: r.gradeAfter,
      motion: r.motion,
    }));

  const summary = {
    version: COMMERCIAL_DEPTH_V2,
    apply: APPLY,
    before: {
      bag: opps.length,
      customerVisible: beforeCf.length,
      contacts: beforeContacts,
      coverage: beforeCoverage,
    },
    after: {
      customerVisible: afterCf.length,
      contacts: afterContacts,
      coverage: afterCoverage,
      byFacing: countBy(afterCf, (o) => o.customerFacingState || o.priority),
      byMotion: countBy(afterCf, (o) => o.commercialMotion || classifyLodgingPrimaryMotion(o, hotelConfig)),
    },
    movement,
    actionableNow: results.filter((r) => r.actionable?.actionable).map((r) => ({
      title: r.title,
      motion: r.motion,
      contact: r.contactAfter,
      person: r.person,
      whyNow: r.whyNow,
      action: r.action,
    })),
    namedContacts,
    lodging: {
      housingPageFound: results.filter((r) => r.lodging.housingPage).length,
      roomBlockEvidence: results.filter((r) => r.lodging.roomBlock).length,
      overflowEvidence: results.filter((r) => r.lodging.overflow).length,
      attendanceKnown: results.filter((r) => r.lodging.attendance != null).length,
      peakRoomsKnown: results.filter((r) => r.lodging.peakRooms != null).length,
    },
    jev,
    adpParity,
    results,
  };

  fs.writeFileSync(path.join(OUT, "COMMERCIAL_DEPTH_V2_SUMMARY.json"), JSON.stringify(summary, null, 2));
  fs.writeFileSync(path.join(OUT, "ADP_SCENARIO_PARITY_AUDIT.json"), JSON.stringify(adpParity, null, 2));
  console.log(JSON.stringify({
    apply: APPLY,
    researched: results.length,
    actionable: summary.actionableNow.length,
    contactsAfter: afterContacts,
    lodging: summary.lodging,
    jev: { calls: jev.calls, safeApply: jev.safeApply, helpful: jev.helpfulDifferent, same: jev.same },
    adpParity: { afterFix: universe.length, rerunCost: adpParity.ESTIMATED_COST_USD },
    movement,
  }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
