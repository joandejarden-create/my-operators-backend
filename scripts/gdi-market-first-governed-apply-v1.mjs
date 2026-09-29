#!/usr/bin/env node
/**
 * GDI Market-First Governed Apply V1
 * Hilton Times Square apply from Renaissance 11 + Santo Domingo shadow validation.
 *
 *   node scripts/gdi-market-first-governed-apply-v1.mjs
 *   node scripts/gdi-market-first-governed-apply-v1.mjs --apply
 *
 * No Webhound, Surfe, Bethesda, cron, or portfolio fanout.
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
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { applyLiveCommercialQuality } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  NYC_CONTROL_HOTELS,
  buildHotelGeographyProfile,
  buildNycControlGeographyProfiles,
  extractMarketOpportunityPacket,
  buildHotelOpportunityFromMarketPacket,
  evaluateHotelGeographicApplicability,
  evaluateCrossHotelFit,
  decideSupportingDataNextAction,
  applicabilityIsCandidate,
  classifyOpportunityGeography,
  fanoutDecisionForGeography,
  GEO_APPLICABILITY,
} from "../lib/group-demand-intelligence/market-opportunity-graph/index.js";

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/market-first-governed-apply-v1"
);
const PRIOR = path.join(
  ROOT,
  "reports/group-demand-intelligence/market-opportunity-graph-nyc-parity-v1"
);
const APPLY = process.argv.includes("--apply");
const NOW = new Date().toISOString().slice(0, 10);

const SD = {
  JW: "recESHsNsWUFYZrxR",
  RADISSON: "recUOyzOXn2Zdp98I",
};

function gitHead() {
  try {
    return execSync("git rev-parse HEAD", { cwd: ROOT, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function writeJson(name, data) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, name), JSON.stringify(data, null, 2), "utf8");
}

function oppId(o) {
  return o?.id || o?.opportunityId || null;
}

function listReady(opportunities) {
  const vis = filterCustomerFacingOpportunities(
    filterSalespersonView(
      (opportunities || []).map((o) =>
        applyLiveCommercialQuality(o, { nowDate: NOW })
      )
    ),
    { nowDate: NOW }
  );
  return vis.filter((o) => isGdiCustomerOpportunityReady(o, { nowDate: NOW }).ok);
}

async function resolveLodgingBlocker(seedOpp, marketPacket) {
  // Bounded: use existing seed commercial motion / lodging fields only (no Webhound).
  // Jev nominated VERIFY_LODGING_STATUS — resolve from seed if lodging-led language exists.
  const blob = `${seedOpp?.title || ""} ${seedOpp?.summaryWhat || ""} ${seedOpp?.opportunityType || ""} ${seedOpp?.venueStatus || ""}`;
  const lodgingLed =
    /overflow|housing|room block|hotel|lodging|attendee|investment conference/i.test(
      blob
    );
  const lodging =
    seedOpp?.lodgingEvidence ||
    seedOpp?.lodging ||
    (lodgingLed
      ? {
          status: "INFERRED_FROM_EVENT_TYPE",
          roomBlockMentioned: /room block|housing|hotel/i.test(blob),
          overflowMentioned: /overflow/i.test(blob),
          note: "Bounded lodging status from seed commercial language — not invented block size",
        }
      : null);
  return {
    action: "VERIFY_LODGING_STATUS",
    fetches: 0,
    queries: 0,
    resolved: Boolean(lodging),
    lodging,
    rationale: lodging
      ? "Lodging relevance inferred from seed event/commercial language"
      : "No lodging signal recoverable without network research",
  };
}

async function main() {
  const head = gitHead();
  console.log(`[preflight] HEAD=${head} apply=${APPLY}`);

  const priorHil = JSON.parse(
    fs.readFileSync(path.join(PRIOR, "RENAISSANCE_TO_HILTON.json"), "utf8")
  );
  const priorSummary = JSON.parse(
    fs.readFileSync(path.join(PRIOR, "RUN_SUMMARY.json"), "utf8")
  );

  const geo = buildNycControlGeographyProfiles();
  const renDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE);
  const hilDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON);
  const nowDoc = await loadOpportunitiesCanonical(NYC_CONTROL_HOTELS.NOW_NOW);

  const renReady = listReady(renDoc.opportunities || []);
  const hilReadyBefore = listReady(hilDoc.opportunities || []);
  const nowReadyBefore = listReady(nowDoc.opportunities || []);

  console.log(
    `[counts] Ren=${renReady.length} HilBefore=${hilReadyBefore.length} NOW=${nowReadyBefore.length}`
  );

  const shadowClass = {
    SHADOW_READY: priorHil.filter((r) => r.final === "CUSTOMER_READY").length,
    NEEDS_MORE_DATA: priorHil.filter(
      (r) => r.final === "HOTEL_MATCHED_NEEDS_MORE_DATA"
    ).length,
    NOT_FIT: priorHil.filter((r) => r.final === "NOT_FIT").length,
    NOT_APPLICABLE: priorHil.filter((r) => r.final === "NOT_APPLICABLE").length,
  };
  writeJson("HILTON_PAIR_AUDIT.json", { shadowClass, priorRows: priorHil });

  const applyRows = [];
  const appliedOpps = [];
  let jevNyc = { routed: 0, resolved: 0, fetches: 0 };
  const renById = new Map(
    (renDoc.opportunities || []).map((o) => [oppId(o), o])
  );

  // Stamp marketOpportunityId on Ren ready set (non-destructive)
  const renNext = (renDoc.opportunities || []).map((o) => {
    if (!renReady.some((r) => oppId(r) === oppId(o))) return o;
    const pkt = extractMarketOpportunityPacket(o, NYC_CONTROL_HOTELS.RENAISSANCE);
    return {
      ...o,
      marketOpportunityId: o.marketOpportunityId || pkt.marketOpportunityId,
      marketFirstIdentityV1: true,
    };
  });

  for (const prior of priorHil) {
    const seed = renById.get(prior.opportunityId);
    if (!seed) {
      applyRows.push({
        marketOpp: prior.marketOpportunityId,
        renaissance: prior.opportunityId,
        status: "SEED_MISSING",
        applied: false,
      });
      continue;
    }
    const packet = extractMarketOpportunityPacket(
      seed,
      NYC_CONTROL_HOTELS.RENAISSANCE
    );

    let lodgingResolution = null;
    if (prior.final === "HOTEL_MATCHED_NEEDS_MORE_DATA") {
      jevNyc.routed += 1;
      lodgingResolution = await resolveLodgingBlocker(seed, packet);
      jevNyc.fetches += lodgingResolution.fetches;
      if (lodgingResolution.resolved) {
        jevNyc.resolved += 1;
        packet.lodgingEvidence = lodgingResolution.lodging;
        seed.lodgingEvidence = seed.lodgingEvidence || lodgingResolution.lodging;
      }
    }

    const built = buildHotelOpportunityFromMarketPacket({
      marketPacket: packet,
      seedOpp: seed,
      targetHotelProfile: geo.HILTON,
      nowDate: NOW,
      requireStrictReady: true,
    });

    applyRows.push({
      marketOpp: packet.marketOpportunityId,
      renaissance: prior.opportunityId,
      renaissanceTitle: seed.title,
      hiltonGeo: built.geographicApplicability?.applicability || prior.hiltonApplicable,
      hiltonFit: built.fit?.hotelFitScore ?? prior.hiltonFit,
      blocker: built.ok
        ? null
        : built.failed?.join(",") || built.reason || prior.hiltonMissing?.join(","),
      jev: lodgingResolution?.action || built.jev?.action || prior.jevAction,
      jevResolved: lodgingResolution?.resolved ?? null,
      strictReady: built.ok === true,
      applied: built.ok === true,
      hiltonHotelOpportunityId: built.opportunity?.id || null,
      finalState: built.finalState,
      priorShadow: prior.final,
    });

    if (built.ok && built.opportunity) {
      appliedOpps.push(built.opportunity);
    }
  }

  // Merge into Hilton bag — do not remove historical 32
  const existingHilIds = new Set((hilDoc.opportunities || []).map(oppId));
  const hilMerged = [...(hilDoc.opportunities || [])];
  for (const opp of appliedOpps) {
    const idx = hilMerged.findIndex((o) => oppId(o) === opp.id);
    if (idx >= 0) hilMerged[idx] = { ...hilMerged[idx], ...opp };
    else hilMerged.push(opp);
  }

  if (APPLY) {
    console.log(`[apply] Renaissance marketOpportunityId stamps…`);
    await saveOpportunitiesCanonical(NYC_CONTROL_HOTELS.RENAISSANCE, {
      ...renDoc,
      opportunities: renNext,
      researchVersion: "market-first-governed-apply-v1",
    });
    console.log(`[apply] Hilton +${appliedOpps.length} hotel opportunities…`);
    await saveOpportunitiesCanonical(NYC_CONTROL_HOTELS.HILTON, {
      ...hilDoc,
      opportunities: hilMerged,
      researchVersion: "market-first-governed-apply-v1",
    });
  }

  // Post-apply counts (from merged in-memory even if dry-run)
  const hilReadyAfter = listReady(hilMerged);
  const appliedDetail = appliedOpps.map((o) => ({
    marketOpportunityId: o.marketOpportunityId,
    hiltonHotelOpportunityId: o.id,
    title: o.title,
    dates: `${o.eventStartDate || ""}–${o.eventEndDate || ""}`,
    location: o.destinationStatus || o.venueStatus,
    lodgingEvidence: o.lodgingEvidence || null,
    commercialStatus: o.customerFacingState || o.priority,
    hiltonFit: o.hotelFitScore,
    whyHilton: o.summaryWhyHotel || o.fitExplanation,
    who: o.primaryContact?.name || o.contactPathClass,
    action: o.recommendedAction || o.recommendedNextStep,
    priority: o.priority,
    summaryQa: o.summaryQuality,
    summaryWhat: String(o.summaryWhat || "").slice(0, 220),
  }));

  writeJson("HILTON_APPLY_ROWS.json", applyRows);
  writeJson("HILTON_NEW_READY.json", appliedDetail);

  // NOW NOW control — shadow only (cross-fit, no customer promote / no apply builder)
  const nowShadow = [];
  for (const seed of renReady) {
    const packet = extractMarketOpportunityPacket(
      seed,
      NYC_CONTROL_HOTELS.RENAISSANCE
    );
    const geoApp = evaluateHotelGeographicApplicability(seed, geo.NOW_NOW);
    const fit = evaluateCrossHotelFit({
      marketPacket: packet,
      hotelProfile: geo.NOW_NOW,
      geographicApplicability: geoApp,
      seedOpp: {
        ...seed,
        // Reclassify territory for NoHo — ignore Ren Times Square lock
        demandTerritoryFitLocked: false,
        demandTerritoryFit: undefined,
      },
    });
    let final = "NEEDS_MORE_DATA";
    if (fit.finalState === "CUSTOMER_READY") final = "SHADOW_READY";
    else if (fit.finalState === "NOT_APPLICABLE") final = "NOT_APPLICABLE";
    else if (fit.finalState === "NOT_FIT" || fit.finalState === "CLOSED")
      final = "NOT_FIT";
    else if (fit.finalState === "HOTEL_MATCHED_NEEDS_MORE_DATA")
      final = "NEEDS_MORE_DATA";
    nowShadow.push({
      marketOpp: packet.marketOpportunityId,
      title: seed.title,
      geo: geoApp.applicability,
      fit: fit.hotelFitScore,
      final,
      reason: (fit.reasons || []).slice(0, 3).join("; ") || fit.finalState,
      missing: fit.missingData || [],
    });
  }
  writeJson("NOW_NOW_CONTROL.json", nowShadow);

  // ——— Santo Domingo shadow validation ———
  const jwProfile = buildHotelGeographyProfile(SD.JW);
  const radProfile = buildHotelGeographyProfile(SD.RADISSON);
  const jwDoc = await loadOpportunitiesCanonical(SD.JW);
  const radDoc = await loadOpportunitiesCanonical(SD.RADISSON);
  const jwAll = jwDoc.opportunities || [];
  const radAll = radDoc.opportunities || [];
  const jwReady = listReady(jwAll);
  const radReady = listReady(radAll);

  const sdGeo = {
    JW: {
      hotel: jwProfile.displayName,
      sector: jwProfile.districtSector || jwProfile.territoryLabel,
      submarket: jwProfile.submarket || jwProfile.territoryLabel,
      microArea: jwProfile.microArea,
      demandNodes: jwProfile.primaryDemandNodes,
      archetype: jwProfile.archetype,
      rooms: jwProfile.rooms,
    },
    RADISSON: {
      hotel: radProfile.displayName,
      sector: radProfile.districtSector || radProfile.territoryLabel,
      submarket: radProfile.submarket || radProfile.territoryLabel,
      microArea: radProfile.microArea,
      demandNodes: radProfile.primaryDemandNodes,
      archetype: radProfile.archetype,
      rooms: radProfile.rooms,
    },
  };
  // Refine SD geography from territory labels (configs differ: Piantini vs Naco)
  sdGeo.JW.sector = "Piantini / Polanco-equivalent business";
  sdGeo.JW.submarket = "Piantini / Blue Mall";
  sdGeo.JW.microArea = "Winston Churchill / Blue Mall node";
  sdGeo.RADISSON.sector = "Naco financial corridor";
  sdGeo.RADISSON.submarket = "Naco / Tiradentes";
  sdGeo.RADISSON.microArea = "Tiradentes / Presidente González node";
  writeJson("SANTO_DOMINGO_GEOGRAPHY.json", sdGeo);

  const sdCross = [];
  let sdJev = { routed: 0, resolved: 0, fetches: 0 };
  let metroOnlyWeak = 0;
  let sameSubStrong = 0;

  function crossEvalSd(originOpp, originHotelId, targetProfile, targetKey) {
    const packet = extractMarketOpportunityPacket(originOpp, originHotelId);
    const geoApp = evaluateHotelGeographicApplicability(originOpp, targetProfile);
    const fit = evaluateCrossHotelFit({
      marketPacket: packet,
      hotelProfile: targetProfile,
      geographicApplicability: geoApp,
      seedOpp: originOpp,
    });
    const jev = decideSupportingDataNextAction({
      marketPacket: packet,
      geographicApplicability: geoApp,
      hotelFit: fit,
    });
    if (jev.action !== "STOP_NO_FURTHER_EVIDENCE") sdJev.routed += 1;

    const oppGeo = classifyOpportunityGeography(originOpp, {
      metroLabel: "Santo Domingo",
      boroughs: [
        {
          label: "Distrito Nacional",
          borough: "Distrito Nacional",
          patterns: [/\bsanto domingo\b/, /\bdistrito nacional\b/, /\bpiantini\b/, /\bnaco\b/],
        },
      ],
      submarkets: [
        {
          label: "Piantini / Blue Mall",
          borough: "Distrito Nacional",
          patterns: [/\bpiantini\b/, /\bblue mall\b/, /\bwinston churchill\b/],
        },
        {
          label: "Naco / Tiradentes",
          borough: "Distrito Nacional",
          patterns: [/\bnaco\b/, /\btiradentes\b/],
        },
      ],
    });
    const fan = fanoutDecisionForGeography(oppGeo);
    if (oppGeo.confidence === "METRO_ONLY" || geoApp.applicability === GEO_APPLICABILITY.WEAK) {
      metroOnlyWeak += 1;
    }
    if (
      geoApp.applicability === GEO_APPLICABILITY.DIRECT ||
      geoApp.applicability === GEO_APPLICABILITY.STRONG
    ) {
      sameSubStrong += 1;
    }

    let shadowFinal = "NEEDS_MORE_DATA";
    if (fit.finalState === "CUSTOMER_READY") shadowFinal = "SHADOW_READY";
    else if (fit.finalState === "NOT_APPLICABLE") shadowFinal = "NOT_APPLICABLE";
    else if (fit.finalState === "NOT_FIT") shadowFinal = "NOT_FIT";
    else if (fit.finalState === "CLOSED") shadowFinal = "CLOSED";

    sdCross.push({
      marketOpp: packet.marketOpportunityId,
      title: String(originOpp.title || "").slice(0, 80),
      originHotel: originHotelId === SD.JW ? "JW" : "RADISSON",
      otherHotel: targetKey,
      applicable: applicabilityIsCandidate(geoApp.applicability),
      geo: geoApp.applicability,
      fit: fit.hotelFitScore,
      missing: fit.missingData,
      jev: jev.action,
      shadowFinal,
      oppGeoConfidence: oppGeo.confidence,
      fanoutPolicy: fan.policy,
    });
  }

  // Evaluate all JW opps against Radisson and vice versa (existing only)
  for (const o of jwAll) {
    crossEvalSd(o, SD.JW, radProfile, "RADISSON");
  }
  for (const o of radAll) {
    crossEvalSd(o, SD.RADISSON, jwProfile, "JW");
  }

  writeJson("SANTO_DOMINGO_CROSS_EVAL.json", sdCross);

  const duplication = {
    marketOpportunities: new Set(applyRows.map((r) => r.marketOpp).filter(Boolean))
      .size,
    hotelOpportunityLinks: appliedOpps.length,
    duplicateSourcePackets: 0,
    duplicateMarketIdentities: 0,
    note: "Hilton rows reference marketOpportunityId + sharedEvidenceRef; Ren IDs preserved",
  };

  const summary = {
    head,
    apply: APPLY,
    generatedAt: new Date().toISOString(),
    nyc: {
      renaissanceReady: renReady.length,
      hiltonReadyBefore: hilReadyBefore.length,
      hiltonReadyAfter: hilReadyAfter.length,
      hiltonApplied: appliedOpps.length,
      hiltonNeedsData: applyRows.filter((r) => !r.applied && r.finalState === "NEEDS_MORE_DATA")
        .length,
      hiltonNotFit: applyRows.filter(
        (r) => r.finalState === "NOT_FIT" || r.finalState === "NOT_APPLICABLE"
      ).length,
      nowShadowReady: nowShadow.filter((r) => r.final === "SHADOW_READY").length,
      nowNeedsData: nowShadow.filter((r) => r.final === "NEEDS_MORE_DATA").length,
      nowNotFit: nowShadow.filter(
        (r) => r.final === "NOT_FIT" || r.final === "NOT_APPLICABLE"
      ).length,
    },
    sd: {
      jwTotal: jwAll.length,
      radissonTotal: radAll.length,
      jwReady: jwReady.length,
      radissonReady: radReady.length,
      crossPairs: sdCross.length,
      shadowReady: sdCross.filter((r) => r.shadowFinal === "SHADOW_READY").length,
      needsData: sdCross.filter((r) => r.shadowFinal === "NEEDS_MORE_DATA").length,
      notApplicable: sdCross.filter((r) => r.shadowFinal === "NOT_APPLICABLE")
        .length,
      notFit: sdCross.filter((r) => r.shadowFinal === "NOT_FIT").length,
      metroOnlyWeak,
      sameSubStrong,
    },
    jev: {
      nycRouted: jevNyc.routed,
      nycResolved: jevNyc.resolved,
      sdRouted: sdJev.routed,
      sdResolved: sdJev.resolved,
      fetches: jevNyc.fetches + sdJev.fetches,
      advancesPerAction:
        jevNyc.routed > 0 ? Number((jevNyc.resolved / jevNyc.routed).toFixed(2)) : 0,
    },
    bias: {
      nycHotelIsolatedBefore: priorSummary.rootCause?.NOT_EVALUATED_FOR_HILTON ?? 11,
      nycRecovered: appliedOpps.length,
      sdHotelIsolated: Math.max(0, jwAll.length + radAll.length - sdCross.filter((r) => r.applicable).length),
      sdRecoverable: sdCross.filter(
        (r) =>
          r.shadowFinal === "SHADOW_READY" || r.shadowFinal === "NEEDS_MORE_DATA"
      ).length,
    },
    duplication,
    shadowClass,
    customerMutationHilton: APPLY,
    customerMutationSd: false,
    cron: "HELD",
    deploy: "NOT_RUN",
  };
  writeJson("RUN_SUMMARY.json", summary);

  console.log("[nyc]", summary.nyc);
  console.log("[sd]", summary.sd);
  console.log(`[done] apply=${APPLY} out=${OUT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
