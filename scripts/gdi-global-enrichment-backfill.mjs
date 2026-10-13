/**
 * Global GDI opportunity enrichment backfill (summary + WHO stamps).
 * Hotel-agnostic. Does not invent people or lodging. Does not change card UI.
 *
 * Usage:
 *   node scripts/gdi-global-enrichment-backfill.mjs
 *   node scripts/gdi-global-enrichment-backfill.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import {
  loadOpportunitiesCanonical,
  saveOpportunitiesCanonical,
} from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import {
  evaluateGdiSummaryQuality,
  applyGdiSummaryEnrichment,
  SUMMARY_QUALITY,
} from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import {
  applyGdiWhoHowResolution,
  classifyWhoHowPath,
  WHO_PATH_CLASS,
} from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import {
  isGdiCustomerOpportunityReady,
  READINESS_HOLD,
} from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  evaluateOpportunityEnrichmentNextStep,
  buildEnrichmentJevPacket,
} from "../lib/group-demand-intelligence/jev-opportunity-enrichment-v1.js";
import { businessDateYmd } from "../lib/group-demand-intelligence/active-eligibility-v1.js";

const APPLY = process.argv.includes("--apply");
const NOW = businessDateYmd();

const HOTELS = [
  { hotelId: "recLuxvwwxID7U2B8", label: "Bethesda Marriott" },
  { hotelId: "recG66DQJKP2c0UNh", label: "Renaissance New York Times Square" },
  { hotelId: "rec35fExUxCClpOP6", label: "Hilton New York Times Square" },
  { hotelId: "rece0or38cxo3Fymb", label: "W Rome" },
];

const outDir = path.join(process.cwd(), "reports", "gdi-global-enrichment-v1");
fs.mkdirSync(outDir, { recursive: true });

const report = {
  generatedAt: new Date().toISOString(),
  businessDate: NOW,
  apply: APPLY,
  hotels: {},
  jev: {
    calls: 0,
    safeApply: 0,
    enrichmentNextStep: 0,
    helpfulDifferent: 0,
    same: 0,
    wrong: 0,
  },
};

for (const hotel of HOTELS) {
  let doc;
  try {
    doc = await loadOpportunitiesCanonical(hotel.hotelId);
  } catch (err) {
    report.hotels[hotel.hotelId] = {
      label: hotel.label,
      error: String(err?.message || err),
    };
    continue;
  }

  const all = doc.opportunities || [];
  const activeBefore = filterCustomerFacingOpportunities(
    filterSalespersonView(all),
    { nowDate: NOW }
  );

  const beforeStats = { STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0 };
  const whoBefore = {
    NAMED_DIRECT: 0,
    NAMED_PARTIAL: 0,
    FUNCTIONAL: 0,
    ORG_PATH: 0,
    NO_CONTACT_AFTER_RESEARCH: 0,
    NOT_RESEARCHED: 0,
  };
  for (const o of activeBefore) {
    beforeStats[evaluateGdiSummaryQuality(o).quality] += 1;
    whoBefore[classifyWhoHowPath(o).pathClass] += 1;
  }

  let summaryUpgrades = 0;
  let whoUpgrades = 0;
  let strongKept = 0;
  let thinRemain = 0;
  const next = all.map((raw) => {
    let opp = { ...raw };
    const beforeQ = evaluateGdiSummaryQuality(opp).quality;
    const beforeWho = classifyWhoHowPath(opp).pathClass;

    // Summary: keep STRONG; re-enrich THIN / title-dup / missing
    if (beforeQ === SUMMARY_QUALITY.STRONG) {
      opp.summaryQuality = SUMMARY_QUALITY.STRONG;
      strongKept += 1;
    } else if (
      beforeQ === SUMMARY_QUALITY.THIN ||
      beforeQ === SUMMARY_QUALITY.INVALID ||
      !opp.summaryWhat
    ) {
      const packet = applyGdiSummaryEnrichment(opp, { force: beforeQ !== SUMMARY_QUALITY.ADEQUATE });
      opp = packet.opportunity;
      if (packet.result.changed) summaryUpgrades += 1;
    } else {
      opp.summaryQuality = beforeQ;
    }

    // WHO: stamp from existing evidence; if still NOT_RESEARCHED mark ceiling
    // (bag already lived through prior research cycles — do not invent people)
    let whoPacket = applyGdiWhoHowResolution(opp);
    opp = whoPacket.opportunity;
    if (whoPacket.classified.pathClass === WHO_PATH_CLASS.NOT_RESEARCHED) {
      whoPacket = applyGdiWhoHowResolution(opp, {
        markAttempted: true,
        ceilingReason: "PUBLIC_DATA_CEILING",
      });
      opp = whoPacket.opportunity;
    }
    if (
      beforeWho === WHO_PATH_CLASS.NOT_RESEARCHED &&
      whoPacket.classified.pathClass !== WHO_PATH_CLASS.NOT_RESEARCHED
    ) {
      whoUpgrades += 1;
    }

    const afterQ = evaluateGdiSummaryQuality(opp).quality;
    if (afterQ === SUMMARY_QUALITY.THIN || afterQ === SUMMARY_QUALITY.INVALID) {
      thinRemain += 1;
    }

    // Jev enrichment routing (SAFE APPLY decision only — no invented facts)
    const jevPacket = buildEnrichmentJevPacket(
      opp,
      { quality: afterQ },
      whoPacket.classified
    );
    const jev = evaluateOpportunityEnrichmentNextStep(jevPacket);
    report.jev.calls += 1;
    if (jev.safeApply) report.jev.safeApply += 1;
    report.jev.enrichmentNextStep += 1;
    opp.enrichmentNextStep = jev.decision;
    opp.enrichmentNextStepRationale = jev.rationale;

    const ready = isGdiCustomerOpportunityReady(opp, { nowDate: NOW });
    opp.customerReadiness = ready;
    // Do not demote previously active STRONG/ADEQUATE Bethesda rows solely for
    // ancillary fit fields if surface eligibility already passed historically.
    if (
      ready.ok === false &&
      ready.state === READINESS_HOLD.HELD_FOR_SUMMARY &&
      opp.customerVisible !== false &&
      afterQ !== SUMMARY_QUALITY.STRONG &&
      afterQ !== SUMMARY_QUALITY.ADEQUATE
    ) {
      opp.customerVisible = false;
      opp.customerActiveEligible = false;
      opp.customerReadinessHold = ready.state;
    }

    return opp;
  });

  const activeAfter = filterCustomerFacingOpportunities(
    filterSalespersonView(next),
    { nowDate: NOW }
  );
  const afterStats = { STRONG: 0, ADEQUATE: 0, THIN: 0, INVALID: 0 };
  const whoAfter = {
    NAMED_DIRECT: 0,
    NAMED_PARTIAL: 0,
    FUNCTIONAL: 0,
    ORG_PATH: 0,
    NO_CONTACT_AFTER_RESEARCH: 0,
    NOT_RESEARCHED: 0,
  };
  let readyCount = 0;
  let heldSummary = 0;
  let heldWho = 0;
  let heldEvidence = 0;
  for (const o of activeAfter) {
    const q = evaluateGdiSummaryQuality(o).quality;
    afterStats[q] += 1;
    whoAfter[classifyWhoHowPath(o).pathClass] += 1;
    const r = o.customerReadiness || isGdiCustomerOpportunityReady(o, { nowDate: NOW });
    if (r.ok) readyCount += 1;
    else if (r.state === READINESS_HOLD.HELD_FOR_SUMMARY) heldSummary += 1;
    else if (r.state === READINESS_HOLD.HELD_FOR_WHO_RESEARCH) heldWho += 1;
    else heldEvidence += 1;
  }

  report.hotels[hotel.hotelId] = {
    label: hotel.label,
    all: all.length,
    activeBefore: activeBefore.length,
    activeAfter: activeAfter.length,
    summaryBefore: beforeStats,
    summaryAfter: afterStats,
    whoBefore,
    whoAfter,
    summaryUpgrades,
    whoUpgrades,
    strongKept,
    thinRemain,
    readiness: {
      ready: readyCount,
      heldSummary,
      heldWho,
      heldEvidence,
    },
  };

  if (APPLY && all.length) {
    await saveOpportunitiesCanonical(hotel.hotelId, {
      ...doc,
      opportunities: next,
      updatedAt: new Date().toISOString(),
      globalEnrichmentV1At: new Date().toISOString(),
      globalEnrichmentBusinessDate: NOW,
    });
  }
}

const outPath = path.join(
  outDir,
  `backfill-${NOW}${APPLY ? "-applied" : "-dry"}.json`
);
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ ok: true, apply: APPLY, outPath, summary: report.hotels, jev: report.jev }, null, 2));
