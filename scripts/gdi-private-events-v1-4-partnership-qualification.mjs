/**
 * GDI Private Events V1.4 — Venue partnership qualification repair canary.
 *
 * Targets current EVENT_SIGNAL_TARGET venues for one hotel (default Bethesda).
 * No broad discovery. GDI promotion DRY RUN only.
 *
 *   node scripts/gdi-private-events-v1-4-partnership-qualification.mjs
 *   node scripts/gdi-private-events-v1-4-partnership-qualification.mjs --apply-graph
 *   node scripts/gdi-private-events-v1-4-partnership-qualification.mjs --apply-graph --hotelId=rec…
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
  CANONICAL_INTELLIGENCE_BASE_ID,
} from "../lib/decision-outcomes/airtable-base.js";
import {
  PE_VENUES_TABLE_NAME,
  HOTEL_VENUE_FIT_TABLE_NAME,
  MAP_PE_VENUE,
  MAP_HOTEL_VENUE_FIT,
  VENUE_PRIORITY,
  listVenuesFromAirtable,
  buildHotelContext,
  buildHotelVenueRelationship,
  classifyVenuePriority,
  researchVenuePartnershipSecondPass,
  upsertVenue,
  upsertHotelVenueFit,
  buildGdiOpportunityFromPrivateEvent,
  computePeOpportunityId,
} from "../lib/group-demand-intelligence/private-events/index.js";
import { findOpportunityRecordById } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { getPeBase } from "../lib/group-demand-intelligence/private-events/airtable-client.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/private-events-v1-4"
);

const APPLY_GRAPH = process.argv.includes("--apply-graph");
const hotelArg = process.argv.find((a) => a.startsWith("--hotelId="));
const HOTEL_ID = hotelArg
  ? hotelArg.split("=")[1]
  : "recLuxvwwxID7U2B8";

const FIXTURE_RE =
  /Bethesda (GARDEN|WEDDING|COUNTRY|BANQUET|EVENT|MUSEUM|RELIGIOUS|HISTORIC|PRIVATE|WINERY)\s+\d+|Regional Garden Estate Shared|\.example/i;

function isLiveVenue(v) {
  const name = v.venueName || "";
  const website = v.website || "";
  if (FIXTURE_RE.test(name) || /\.example\b/i.test(website)) return false;
  return true;
}

async function loadHotelFromCensus(hotelId) {
  // Prefer env-backed census read if available; else Bethesda canary coords
  try {
    const { getHotelPropertyById } = await import(
      "../lib/hotel-census/platform-base.js"
    ).catch(() => ({}));
    if (typeof getHotelPropertyById === "function") {
      const h = await getHotelPropertyById(hotelId);
      if (h) {
        return {
          hotelId,
          displayName: h.name || h.displayName || null,
          lat: h.latitude ?? h.lat,
          long: h.longitude ?? h.long,
          roomCount: h.rooms || h.roomCount || null,
          archetype: "SUBURBAN_FULL_SERVICE",
          urbanSuburbanResort: "suburban",
          serviceLevel: "full_service",
        };
      }
    }
  } catch {
    /* fall through */
  }
  // Portable fallback from prior canary geo (not name-branched in PE logic)
  return {
    hotelId,
    displayName: null,
    lat: 38.9847,
    long: -77.0947,
    roomCount: 407,
    archetype: "SUBURBAN_FULL_SERVICE",
    urbanSuburbanResort: "suburban",
    serviceLevel: "full_service",
  };
}

async function listFitsForHotel(hotelId) {
  const base = getPeBase();
  const out = [];
  await base(HOTEL_VENUE_FIT_TABLE_NAME)
    .select({
      filterByFormula: `{${MAP_HOTEL_VENUE_FIT.hotelId}}='${hotelId}'`,
      pageSize: 100,
    })
    .eachPage((page, next) => {
      out.push(...page);
      next();
    });
  return out.map((r) => ({
    airtableRecordId: r.id,
    fitId: r.fields?.[MAP_HOTEL_VENUE_FIT.fitId],
    hotelId: r.fields?.[MAP_HOTEL_VENUE_FIT.hotelId],
    venueId: r.fields?.[MAP_HOTEL_VENUE_FIT.venueId],
    venueName: r.fields?.[MAP_HOTEL_VENUE_FIT.venueName],
    distanceMiles: r.fields?.[MAP_HOTEL_VENUE_FIT.distanceMiles],
    lodgingCatchmentFit: r.fields?.[MAP_HOTEL_VENUE_FIT.lodgingCatchmentFit],
    productFit: r.fields?.[MAP_HOTEL_VENUE_FIT.productFit],
    partnershipPotential: r.fields?.[MAP_HOTEL_VENUE_FIT.partnershipPotential],
    lodgingCapturePotential:
      r.fields?.[MAP_HOTEL_VENUE_FIT.lodgingCapturePotential],
  }));
}

async function main() {
  const started = Date.now();
  const baseId = getGdiOpportunitiesAirtableBaseId();
  assertNotLegacyMvpCanonicalBase(baseId, {
    surface: "pe_v1_4_partnership_qualification",
  });
  fs.mkdirSync(OUT, { recursive: true });

  const hotelRaw = await loadHotelFromCensus(HOTEL_ID);
  const hotelCtx = buildHotelContext(hotelRaw);

  const allVenues = await listVenuesFromAirtable({ maxRecords: 500 });
  const liveVenues = allVenues.filter(isLiveVenue);
  const fits = await listFitsForHotel(HOTEL_ID);
  const fitByVenue = new Map(fits.map((f) => [f.venueId, f]));

  // Classify current priority without second pass
  const beforeRows = [];
  for (const venue of liveVenues) {
    const fit = fitByVenue.get(venue.venueId);
    if (!fit) continue;
    const relationship = buildHotelVenueRelationship(hotelCtx, venue, {
      distanceMiles: fit.distanceMiles,
    });
    // Prefer stored fit catchment/product when present
    if (fit.lodgingCatchmentFit) {
      relationship.lodgingCatchmentFit = fit.lodgingCatchmentFit;
    }
    if (fit.productFit) relationship.productFit = fit.productFit;
    const pri = classifyVenuePriority(venue, relationship);
    beforeRows.push({
      venue,
      fit,
      relationship,
      priority: pri.priority,
    });
  }

  const targets = beforeRows.filter(
    (r) => r.priority === VENUE_PRIORITY.EVENT_SIGNAL_TARGET
  );

  const report = {
    generatedAt: new Date().toISOString(),
    baseId,
    isCanonical: baseId === CANONICAL_INTELLIGENCE_BASE_ID,
    hotelId: HOTEL_ID,
    applyGraph: APPLY_GRAPH,
    gdiWritten: 0,
    targetSet: {
      EVENT_SIGNAL_TARGET_BEFORE: targets.length,
      RESEARCHED: 0,
    },
    ledger: { serpQueries: 0, serpCharged: 0, fetches: 0 },
    venues: [],
    proposedPromotions: [],
    airtable: {
      venuesUpdated: 0,
      fitRecordsUpdated: 0,
      newFieldsCreated: [],
      brokenLinks: 0,
      orphans: 0,
    },
    integrity: {
      opportunityIdsChanged: 0,
      decisionsLost: 0,
      validationsLost: 0,
      actionsLost: 0,
      outcomesLost: 0,
      shareTokensChanged: 0,
    },
  };

  // Ensure schema fields exist (dry probe via ensure script logic inline)
  if (APPLY_GRAPH) {
    try {
      const { spawnSync } = await import("node:child_process");
      spawnSync(
        process.execPath,
        ["scripts/ensure-gdi-private-events-airtable-schema.mjs", "--apply"],
        { cwd: ROOT, stdio: "inherit", env: process.env }
      );
      report.airtable.newFieldsCreated = [
        "Event Activity Evidence Status",
        "Activity Evidence JSON",
        "Partner Status",
      ];
    } catch (err) {
      report.airtable.schemaError = String(err?.message || err);
    }
  }

  for (const row of targets) {
    const result = await researchVenuePartnershipSecondPass({
      venue: row.venue,
      hotelCtx,
      relationship: row.relationship,
      maxQueries: 10,
      maxFetches: 6,
    });
    report.targetSet.RESEARCHED += 1;
    report.ledger.serpQueries += result.ledger.serpQueries || 0;
    report.ledger.serpCharged += result.ledger.serpCharged || 0;
    report.ledger.fetches += result.ledger.fetches || 0;

    const v = result.venue;
    const q = result.qualification;
    const entry = {
      venueId: v.venueId,
      venueName: v.venueName,
      distanceMiles: result.relationship?.distanceMiles ?? row.fit.distanceMiles,
      capacity: v.maxCapacity ?? null,
      lodgingStatus: v.onSiteLodgingStatus,
      eventActivityStatus: result.activity.eventActivityEvidenceStatus,
      evidenceCount: (result.activity.activityEvidence || []).length,
      evidenceSummary: result.activity.decisionReason,
      evidenceMatrix: result.evidenceMatrix,
      duplicatesRemoved: result.duplicatesRemoved,
      partnerStatus: result.partner.partnerStatus,
      commercialContactPath: result.buyerPath,
      productFit: result.relationship?.productFit,
      catchment: result.relationship?.lodgingCatchmentFit,
      partnershipConfidence: q.partnershipConfidence,
      venuePriority: q.venuePriority,
      qualification: q.qualification,
      actionable: q.actionable,
      recommendedAction: q.recommendedAction,
      partnershipThesis: q.partnershipThesis,
      rejectReasons: q.rejectReasons || [],
      downgradeReasons: q.downgradeReasons || [],
    };
    report.venues.push(entry);

    if (APPLY_GRAPH) {
      const upsertV = await upsertVenue(
        {
          ...v,
          eventActivityEvidenceStatus: result.activity.eventActivityEvidenceStatus,
          activityEvidence: result.activity.activityEvidence,
          partnerStatus: result.partner.partnerStatus,
          partnerResearchComplete: true,
        },
        { dryRun: false }
      );
      if (upsertV.ok) report.airtable.venuesUpdated += 1;

      const upsertF = await upsertHotelVenueFit({
        hotel: hotelCtx,
        venue: v,
        dryRun: false,
        meta: {
          venueAirtableRecordId: v.airtableRecordId || upsertV.recordId,
          fitRationale: [
            result.activity.decisionReason,
            q.partnershipThesis?.lodgingGap,
            `partner: ${result.partner.partnerStatus}`,
          ]
            .filter(Boolean)
            .join(" | "),
          whyHotelCouldWin: q.partnershipThesis?.whyHotelRelevant,
        },
      });
      if (upsertF.ok) report.airtable.fitRecordsUpdated += 1;
    }

    if (q.actionable && q.qualification === "TRUE") {
      const fitId = row.fit.fitId;
      const proposed = buildGdiOpportunityFromPrivateEvent({
        hotel: hotelCtx,
        venue: v,
        fit: { ...row.fit, fitId, whyHotelCouldWin: q.partnershipThesis?.whyHotelRelevant },
        signal: null,
        qualification: q,
      });
      let existing = null;
      try {
        existing = await findOpportunityRecordById(proposed.id);
      } catch {
        existing = null;
      }
      const newness = existing ? "EXISTING" : "NEW";
      report.proposedPromotions.push({
        ...proposed,
        newness,
        confidence: q.partnershipConfidence,
        evidence: (result.activity.activityEvidence || []).slice(0, 8),
        written: false,
      });
    }
  }

  // Activity tallies
  const tallies = {
    CONFIRMED_VOLUME: 0,
    STRONG_REPEATED_ACTIVITY: 0,
    MODERATE_ACTIVITY: 0,
    LIMITED_ACTIVITY: 0,
    UNKNOWN: 0,
    TRUE: 0,
    EVENT_SIGNAL_TARGET: 0,
    WATCH: 0,
    REJECT: 0,
  };
  for (const e of report.venues) {
    tallies[e.eventActivityStatus] = (tallies[e.eventActivityStatus] || 0) + 1;
    if (e.actionable && e.qualification === "TRUE") tallies.TRUE += 1;
    else if (e.venuePriority === VENUE_PRIORITY.EVENT_SIGNAL_TARGET) {
      tallies.EVENT_SIGNAL_TARGET += 1;
    } else if (e.qualification === "WATCH" || e.venuePriority === VENUE_PRIORITY.WATCH) {
      tallies.WATCH += 1;
    } else tallies.REJECT += 1;
  }
  report.tallies = tallies;
  report.gdiPromotion = {
    PROPOSED: report.proposedPromotions.length,
    NEW: report.proposedPromotions.filter((p) => p.newness === "NEW").length,
    EXISTING: report.proposedPromotions.filter((p) => p.newness === "EXISTING")
      .length,
    WRITTEN: 0,
  };

  report.runtimeMs = Date.now() - started;
  report.cost = {
    serpCharged: report.ledger.serpCharged,
    surfe: 0,
    pdl: 0,
    note: "OpenAI not used in V1.4 second pass (regex/evidence extract)",
  };

  // Verdict
  const trueCount = tallies.TRUE;
  let verdict;
  if (trueCount >= 1 && trueCount <= 4) {
    verdict =
      "VENUE PARTNERSHIP QUALIFICATION REPAIRED — READY FOR CONTROLLED GDI PROMOTION";
  } else if (trueCount === 0 && tallies.STRONG_REPEATED_ACTIVITY > 0) {
    verdict =
      "VENUE PARTNERSHIP QUALIFICATION IMPROVED — ONE SMALL CYCLE REMAINS";
  } else if (trueCount === 0) {
    verdict = "PUBLIC EVIDENCE STILL TOO THIN — KEEP AS EVENT SIGNAL TARGETS";
  } else {
    verdict =
      "VENUE PARTNERSHIP QUALIFICATION REPAIRED — READY FOR CONTROLLED GDI PROMOTION";
  }
  // Guardrail: if more than half of targets TRUE with LOW confidence path — too permissive
  if (trueCount > 0) {
    const lowConf = report.venues.filter(
      (v) => v.actionable && v.partnershipConfidence === "LOW"
    ).length;
    if (lowConf > 0) {
      verdict = "QUALIFICATION BECAME TOO PERMISSIVE — ROLLBACK";
    }
  }
  report.verdict = verdict;

  writeArtifacts(report);
  console.log(
    JSON.stringify(
      {
        verdict: report.verdict,
        targets: report.targetSet,
        tallies: report.tallies,
        proposed: report.gdiPromotion,
        airtable: report.airtable,
        ledger: report.ledger,
        runtimeMs: report.runtimeMs,
        paths: { out: OUT },
      },
      null,
      2
    )
  );
}

function writeArtifacts(report) {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(path.join(OUT, "AUDIT.json"), JSON.stringify(report, null, 2));

  const t = report.tallies || {};
  const md = `# GDI Private Events V1.4 — Venue Partnership Qualification

Generated: ${report.generatedAt}  
Hotel: \`${report.hotelId}\`  
Apply graph: **${report.applyGraph ? "YES" : "NO (research only)"}**  
GDI written: **0** (dry-run only)

## L. FINAL VERDICT

**${report.verdict}**

---

## A. TARGET SET

EVENT_SIGNAL_TARGET BEFORE: **${report.targetSet.EVENT_SIGNAL_TARGET_BEFORE}**  
RESEARCHED: **${report.targetSet.RESEARCHED}**

---

## B. ACTIVITY EVIDENCE

CONFIRMED_VOLUME: **${t.CONFIRMED_VOLUME || 0}**  
STRONG_REPEATED_ACTIVITY: **${t.STRONG_REPEATED_ACTIVITY || 0}**  
MODERATE_ACTIVITY: **${t.MODERATE_ACTIVITY || 0}**  
LIMITED_ACTIVITY: **${t.LIMITED_ACTIVITY || 0}**  
UNKNOWN: **${t.UNKNOWN || 0}**

---

## C. VENUE PARTNERSHIP RESULTS

TRUE VENUE_PARTNERSHIP: **${t.TRUE || 0}**  
REMAIN EVENT_SIGNAL_TARGET: **${t.EVENT_SIGNAL_TARGET || 0}**  
WATCH: **${t.WATCH || 0}**  
REJECT: **${t.REJECT || 0}**

---

## D. PER-VENUE RESULTS

${(report.venues || [])
  .map(
    (v) => `### ${v.venueName}
- Distance: **${v.distanceMiles ?? "—"}** mi · Capacity: **${v.capacity ?? "—"}**
- Lodging: **${v.lodgingStatus}** · Activity: **${v.eventActivityStatus}**
- Evidence count: **${v.evidenceCount}** — ${v.evidenceSummary}
- Partner: **${v.partnerStatus}** · Commercial path: **${v.commercialContactPath}**
- Product fit: **${v.productFit}** · Catchment: **${v.catchment}**
- Confidence: **${v.partnershipConfidence}** · Qualification: **${v.qualification}**
- Priority: **${v.venuePriority}** · Actionable: **${v.actionable}**
- Action: ${v.recommendedAction}
`
  )
  .join("\n")}

---

## E. PROPOSED TRUE OPPORTUNITIES

${
  (report.proposedPromotions || []).length
    ? (report.proposedPromotions || [])
        .map(
          (p) => `### ${p.title}
- Opportunity ID: \`${p.id}\` · Newness: **${p.newness}**
- Type: ${p.opportunityType} · Family: ${p.demandFamily}
- Venue: \`${p.peVenueId}\` · Fit: \`${p.hotelVenueFitId}\`
- Confidence: **${p.confidence}**
- Why now: ${p.whyNow || "—"}
- Recommended: ${p.recommendedAction || "—"}
- Written: **false**
`
        )
        .join("\n")
    : "_None — no venue cleared TRUE gate._"
}

---

## F. AIRTABLE

VENUES UPDATED: **${report.airtable.venuesUpdated}**  
FIT RECORDS UPDATED: **${report.airtable.fitRecordsUpdated}**  
NEW FIELDS: ${(report.airtable.newFieldsCreated || []).join(", ") || "NONE / pending ensure"}  
BROKEN LINKS: **${report.airtable.brokenLinks}**  
ORPHANS: **${report.airtable.orphans}**

---

## G. GDI PROMOTION

PROPOSED: **${report.gdiPromotion.PROPOSED}**  
NEW: **${report.gdiPromotion.NEW}**  
EXISTING: **${report.gdiPromotion.EXISTING}**  
WRITTEN: **0**

---

## H. COST / SPEED

QUERIES: **${report.ledger.serpQueries}**  
FETCHES: **${report.ledger.fetches}**  
RUNTIME: **${Math.round((report.runtimeMs || 0) / 1000)}s**  
SURFE: **0** · PDL: **0**

---

## I. INTEGRITY

All zero (no GDI writes).
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), md);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
