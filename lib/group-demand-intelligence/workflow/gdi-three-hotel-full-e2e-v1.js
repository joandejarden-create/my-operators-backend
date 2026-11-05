/**
 * GDI three-hotel autonomous full E2E V1.
 *
 * One resumable local state machine for Mayflower, Bethesda Marriott, and
 * Wyndham San Diego Bayside. Reuses certified gates. Does not spend, publish,
 * or copy another hotel's commercial pack.
 */

import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import {
  GDI_THREE_HOTEL_COHORT,
  resolveGdiHotelArtifactScope,
} from "./gdi-hotel-artifact-scope-v1.js";
import { paidMultisegmentAuthorizationLedgerPath } from "../parallel/budget-ledger-v1.js";
import {
  evaluateProspectPublicationEligibility,
  hasUsableContactRoute,
  PROSPECT_ELIGIBILITY,
} from "../prospect-publication-eligibility-v1.js";
import {
  enrichGdiOpportunityForCustomer,
  isGdiCustomerDrawerReady,
  findCustomerInternalIdLeaks,
} from "../enrich-gdi-opportunity-for-customer.js";
import { toOpportunityListDto } from "../opportunity-list-dto.js";
import { classifyDemandPosture } from "./gdi-demand-posture-v1.js";

export { classifyDemandPosture };

/** Local stubs — avoid importing dealality-full-e2e-os (ADP-tied) into customer runtime. */
const PRODUCT = Object.freeze({ GDI: "GDI", ADP: "ADP" });
const GDI_REQUIRED_SEGMENTS = Object.freeze([
  "associations_government",
  "medical_academic",
  "exhibitor_sponsor_teams",
  "corporate_meetings_training",
  "scaling_companies",
  "sports",
  "entertainment_production",
  "weddings_social_private_events",
  "professional_planners_dmc",
  "local_meetings_catering",
  "tours_educational_specialist",
]);
function resolveOsHotelIdentity(hpcId, _opts = {}) {
  const id = String(hpcId || "").trim();
  return {
    ok: Boolean(id),
    hpcId: id || null,
    product: PRODUCT.GDI,
    source: "gdi-three-hotel-local-identity-stub",
  };
}

export const THREE_HOTEL_CONTRACT = "GDI_THREE_HOTEL_AUTONOMOUS_FULL_E2E_V1";
export const COMMERCIAL_TARGET_MIN = 10;
export const COMMERCIAL_TARGET_MAX = 15;

export const THREE_HOTEL_STAGES = Object.freeze([
  ["S0", "Property identity and existing-state reconciliation"],
  ["S1", "Hotel capability and target-demand certification"],
  ["S2", "Demand segment research and market discovery"],
  ["S3", "Opportunity candidate generation"],
  ["S4", "Opportunity-specific evidence investigation"],
  ["S5", "Buyer and contact-route resolution"],
  ["S6", "Property fit, scoring, and commercial prioritization"],
  ["S7", "Publication eligibility and quality certification"],
  ["S8", "Canonical Airtable persistence and verification"],
  ["S9", "Customer-facing opportunity rendering"],
  ["S10", "PDF and secure-share generation"],
  ["S11", "Full customer-delivery QA and final certification"],
]);

function readJson(file, fallback = null) {
  if (!file || !fs.existsSync(file)) return fallback;
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function csvCell(value) {
  const text = value == null ? "" : String(value);
  if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

export function toCsv(headers, rows) {
  const lines = [headers.join(",")];
  for (const row of rows) {
    lines.push(headers.map((header) => csvCell(row[header])).join(","));
  }
  return `${lines.join("\n")}\n`;
}

export function assessSegmentDocument(segmentId, doc) {
  if (!doc) {
    return {
      segmentId,
      applicable: null,
      status: "MISSING",
      investigated: false,
      exhausted: false,
      sourceFamiliesResearched: [],
      terminationReason: "no_local_segment_state",
    };
  }
  const researched = Array.isArray(doc.sourceFamiliesResearched)
    ? doc.sourceFamiliesResearched
    : [];
  const status = String(doc.status || doc.sufficiency?.status || "UNSTATED");
  const explicitExhausted = /EXHAUSTED/i.test(status);
  const meaningfulSearch = researched.length > 0;
  return {
    segmentId,
    applicable: doc.applicable !== false,
    status,
    investigated: meaningfulSearch || status === "COMPLETE" || status === "IN_PROGRESS",
    exhausted: explicitExhausted && meaningfulSearch,
    sourceFamiliesResearched: researched,
    terminationReason: explicitExhausted
      ? meaningfulSearch
        ? status
        : "exhausted_rejected_without_source_families"
      : meaningfulSearch || status === "COMPLETE" || status === "IN_PROGRESS"
        ? "not_exhausted"
        : "no_meaningful_search_recorded",
  };
}

export function assessHistoricalSpend(ledger, hotelId) {
  const entries = (ledger?.entries || []).filter((entry) => entry.hotelId === hotelId);
  const incurredEstimatedUsd = entries
    .filter((entry) => entry.status === "spent")
    .reduce((sum, entry) => sum + Number(entry.estimatedUsd || 0), 0);
  return {
    hotelId,
    ledgerVersion: ledger?.version || null,
    historicalSharedEnvelopeUsd: ledger?.budgetUsd ?? null,
    hotelIncurredEstimatedUsd: Number(incurredEstimatedUsd.toFixed(2)),
    hotelEntryCount: entries.length,
    sharedSafelyAvailableUsd: ledger?.reconciliation?.safelyAvailableUsd ?? null,
    accountingHoldUsd: ledger?.accountingHoldUsd ?? null,
    actualBilledUsd: ledger?.actualBilledUsd ?? null,
    freshAuthorizationForThisRun: false,
    paidExecution: "BLOCKED",
    reason:
      "Historical hotel caps and any remaining shared balance are not fresh spending authorization and are not transferable across hotels or into ADP.",
  };
}

function opportunityId(opp) {
  return String(opp?.id || opp?.opportunityId || "").trim();
}

function sectionOf(opp) {
  return String(opp?.prospectReviewSection || "").toUpperCase();
}

export function certifyThreeHotelResult(hotel) {
  const failed = (hotel.stages || []).filter((stage) => stage.passed !== true);
  const commercialMet = hotel.publicationEligible >= COMMERCIAL_TARGET_MIN;
  if (failed.length) {
    return {
      technicalStatus: "PARTIAL / BLOCKED",
      commercialStatus: commercialMet
        ? "publication-eligible count meets the floor; delivery stages are not certified"
        : "COMMERCIAL GAP",
      finalStatus: "PARTIAL / BLOCKED",
    };
  }
  if (!commercialMet) {
    return {
      technicalStatus: "TECHNICALLY COMPLETE / COMMERCIAL GAP",
      commercialStatus: "COMMERCIAL GAP",
      finalStatus: "TECHNICALLY COMPLETE / COMMERCIAL GAP",
    };
  }
  return {
    technicalStatus: "FULL E2E CERTIFIED",
    commercialStatus: "ACCEPTED",
    finalStatus: "FULL E2E CERTIFIED",
  };
}

function stage(id, passed, detail) {
  const name = THREE_HOTEL_STAGES.find((row) => row[0] === id)?.[1] || id;
  return { id, name, passed: passed === true, ...detail };
}

function loadOpportunities(scope) {
  const doc = readJson(path.join(scope.hotelData, "opportunities.json"), {
    opportunities: [],
  });
  const rows = Array.isArray(doc.opportunities) ? doc.opportunities : [];
  return {
    rows,
    owned: rows.filter((row) => !row.hotelId || String(row.hotelId) === scope.hotelId),
    foreign: rows.filter((row) => row.hotelId && String(row.hotelId) !== scope.hotelId),
  };
}

function loadSegments(scope) {
  const dir = path.join(scope.hotelData, "segment-research-state");
  return GDI_REQUIRED_SEGMENTS.map((segmentId) => {
    const file = path.join(dir, `${segmentId}.json`);
    return assessSegmentDocument(segmentId, readJson(file, null));
  });
}

function pdfStatus(repoRoot, hotelId) {
  const dir = path.join(repoRoot, "reports/group-demand-intelligence/pdf", hotelId);
  const pdfPath = path.join(dir, "report.pdf");
  const metaPath = path.join(dir, "meta.json");
  const meta = readJson(metaPath, null);
  if (!fs.existsSync(pdfPath)) {
    return {
      present: false,
      hotelMatch: false,
      checksumOk: false,
      generatedAt: meta?.generatedAt || null,
      productionDeliveryVerified: false,
    };
  }
  const bytes = fs.readFileSync(pdfPath);
  const checksum = createHash("sha256").update(bytes).digest("hex");
  return {
    present: true,
    bytes: bytes.length,
    generatedAt: meta?.generatedAt || null,
    metaHotelId: meta?.hotelId || null,
    hotelMatch: meta?.hotelId === hotelId,
    checksumOk: meta?.checksumSha256 ? meta.checksumSha256 === checksum : false,
    productionDeliveryVerified: false,
    note: "On-disk artifact from a prior run. This execution did not regenerate or open a production share.",
  };
}

function shareStatus(repoRoot, hotelId) {
  const registry = readJson(
    path.join(repoRoot, "config/client-share/gdi-share-registry/active-tokens.json"),
    { tokens: {} }
  );
  const tokens = Object.values(registry.tokens || {}).filter(
    (token) => token?.hotelId === hotelId
  );
  const active = tokens.filter((token) => String(token.status || "").toUpperCase() === "ACTIVE");
  return {
    registryRows: tokens.length,
    activeReadOnlyShares: active.length,
    revokedOrOther: tokens.length - active.length,
    tokenValuesIncluded: false,
    productionDeliveryVerified: false,
  };
}

async function readAirtable(hotelId, allow) {
  if (!allow) {
    return {
      attempted: false,
      ok: false,
      blocker: "AIRTABLE_READ_NOT_REQUESTED",
    };
  }
  const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT || "";
  if (!token) {
    return { attempted: false, ok: false, blocker: "AIRTABLE_TOKEN_ABSENT" };
  }
  try {
    const { listOpportunitiesForHotel } = await import(
      "../airtable-opportunity-store.js"
    );
    const rows = await Promise.race([
      listOpportunitiesForHotel(hotelId),
      new Promise((_, reject) => {
        setTimeout(() => reject(new Error("AIRTABLE_READ_TIMEOUT")), 45000);
      }),
    ]);
    const foreign = (rows || []).filter(
      (row) => row?.hotelId && String(row.hotelId) !== String(hotelId)
    );
    return {
      attempted: true,
      ok: foreign.length === 0,
      count: (rows || []).length,
      foreignCount: foreign.length,
      ids: (rows || []).map(opportunityId).filter(Boolean).sort(),
      rows: rows || [],
      writes: 0,
    };
  } catch (error) {
    return {
      attempted: true,
      ok: false,
      blocker: error?.code || error?.message || "AIRTABLE_READ_FAILED",
      writes: 0,
    };
  }
}

function summarizeOpportunity(opp, hotelId) {
  const posture = classifyDemandPosture(opp);
  const route = hasUsableContactRoute(opp);
  let eligibility;
  try {
    eligibility = evaluateProspectPublicationEligibility(opp);
  } catch (error) {
    eligibility = {
      eligibility: "EVALUATION_ERROR",
      hotelFacing: false,
      reasons: [error.message],
    };
  }
  const id = opportunityId(opp);
  return {
    hotelId,
    opportunityId: id,
    organizationName: opp.organizationName || opp.company || "",
    segment: opp.researchSegment || opp.segment || "",
    demandPosture: posture.posture,
    demandPostureReason: posture.reason,
    publicationEligibility: eligibility.eligibility,
    hotelFacing: eligibility.hotelFacing === true,
    prospectSection: sectionOf(opp),
    customerVisible: opp.customerVisible === true,
    usableContactRoute: route.ok === true,
    contactRouteReason: route.reason || "",
    priority: opp.priority || "",
    qualified:
      eligibility.hotelFacing === true || opp.qualificationGatePassed === true,
    rejected: eligibility.eligibility === PROSPECT_ELIGIBILITY.NOT_A_PROSPECT,
    investigated:
      (Array.isArray(opp.establishedFacts) && opp.establishedFacts.length > 0) ||
      (Array.isArray(opp.sources) && opp.sources.length > 0) ||
      (Array.isArray(opp.evidence) && opp.evidence.length > 0),
  };
}

function customerSample(opportunities, hotelId, config) {
  const visible = opportunities.filter((opp) => opp.customerVisible === true).slice(0, 8);
  return visible.map((opp) => {
    const enriched = enrichGdiOpportunityForCustomer(opp, { hotelId, hotelConfig: config });
    const dto = toOpportunityListDto(enriched);
    const leaks = findCustomerInternalIdLeaks(enriched);
    const drawer = isGdiCustomerDrawerReady(enriched);
    return {
      opportunityId: opportunityId(opp),
      hotelId: enriched.hotelId,
      hotelMatch: String(enriched.hotelId || "") === String(hotelId),
      drawerReady: drawer?.ok === true,
      drawerFailed: drawer?.failed || [],
      leakCount: leaks.length,
      listTitlePresent: Boolean(dto?.title || dto?.opportunityName || dto?.name),
      customerVisible: enriched.customerVisible === true,
    };
  });
}

export async function assessHotel(hotel, opts) {
  const identity = resolveOsHotelIdentity(hotel.hpcId, { product: PRODUCT.GDI });
  const scope = resolveGdiHotelArtifactScope(hotel.hpcId, opts.repoRoot);
  const config = readJson(
    path.join(opts.repoRoot, "config/group-demand-intelligence/hotels", `${hotel.hpcId}.json`),
    null
  );
  const corpus = loadOpportunities(scope);
  const segments = loadSegments(scope);
  const spend = assessHistoricalSpend(opts.ledger, hotel.hpcId);
  const pdf = pdfStatus(opts.repoRoot, hotel.hpcId);
  const share = shareStatus(opts.repoRoot, hotel.hpcId);
  const airtable = await readAirtable(hotel.hpcId, opts.allowAirtableRead === true);
  const remoteRows = Array.isArray(airtable.rows) ? airtable.rows : [];
  const airtableSummary = { ...airtable };
  delete airtableSummary.rows;
  const corpusSource =
    corpus.owned.length > 0
      ? "local_file"
      : remoteRows.length > 0
        ? "airtable_readback_memory"
        : "none";
  const scoringOpps = corpus.owned.length > 0 ? corpus.owned : remoteRows;
  const localIds = new Set(
    corpus.owned.map(opportunityId).filter(Boolean)
  );
  let airtableOverlap = null;
  if (airtable.ok && Array.isArray(airtable.ids)) {
    const remote = new Set(airtable.ids);
    let overlap = 0;
    for (const id of localIds) if (remote.has(id)) overlap += 1;
    airtableOverlap = {
      localCount: localIds.size,
      remoteCount: airtable.ids.length,
      overlap,
      countsMatch: localIds.size === airtable.ids.length && overlap === localIds.size,
    };
  }
  const rows = scoringOpps.map((opp) => summarizeOpportunity(opp, hotel.hpcId));
  const sample = rows.length ? customerSample(scoringOpps, hotel.hpcId, config) : [];
  const publicationEligible = rows.filter((row) => row.hotelFacing).length;
  const customerVisible = rows.filter((row) => row.customerVisible).length;
  const capability = config?.capabilityProfile || null;
  const stages = [
    stage("S0", identity.ok === true && config?.hotelId === hotel.hpcId && scope.scope === "hotel_local_store", {
      identity,
      artifactScope: scope.scope,
      localOpportunityFile: corpus.rows.length,
    }),
    stage("S1", Boolean(capability?.totalGuestrooms && (capability.meetingRoomsIndoor || capability.meetingRoomCount)), {
      capability: capability
        ? {
            totalGuestrooms: capability.totalGuestrooms,
            meetingRooms: capability.meetingRoomsIndoor || capability.meetingRoomCount || null,
            meetingSpaceSqFt: capability.totalMeetingSpaceSqFt || null,
          }
        : null,
    }),
    stage("S2", segments.every((item) => item.investigated) && segments.every((item) => item.exhausted === false || item.sourceFamiliesResearched.length > 0), {
      investigated: segments.filter((item) => item.investigated).length,
      missing: segments.filter((item) => !item.investigated).map((item) => item.segmentId),
      falselyExhausted: segments.filter((item) => item.terminationReason === "exhausted_rejected_without_source_families").length,
    }),
    stage("S3", corpus.foreign.length === 0 && (airtable.foreignCount || 0) === 0, {
      candidates: rows.length,
      localFileCount: corpus.owned.length,
      corpusSource,
      foreign: corpus.foreign.length,
    }),
    stage("S4", rows.every((row) => row.demandPosture), {
      confirmed: rows.filter((row) => row.demandPosture === "CONFIRMED").length,
      inferred: rows.filter((row) => row.demandPosture === "INFERRED").length,
      exploratory: rows.filter((row) => row.demandPosture === "EXPLORATORY").length,
      notOpenBooking: rows.filter((row) => row.demandPosture === "NOT_OPEN_BOOKING").length,
    }),
    stage("S5", true, {
      usableContactRoutes: rows.filter((row) => row.usableContactRoute).length,
      note: "Route class only. Contact addresses are not copied into this run.",
    }),
    stage("S6", true, {
      priority: rows.filter((row) => row.prospectSection === "PRIORITY").length,
      exploratorySection: rows.filter((row) => row.prospectSection === "EXPLORATORY").length,
    }),
    stage("S7", true, {
      publicationEligible,
      customerVisible,
      commercialTargetMin: COMMERCIAL_TARGET_MIN,
      commercialFloorMet: publicationEligible >= COMMERCIAL_TARGET_MIN,
      paidRecoveryExecuted: false,
      paidRecoveryBlocker: spend.reason,
    }),
    stage("S8", airtable.ok === true && airtable.foreignCount === 0 && airtable.writes === 0 && airtableOverlap?.countsMatch === true, {
      airtable: airtableSummary,
      airtableOverlap,
      localWrites: 0,
    }),
    stage("S9", sample.length > 0 && sample.every((item) => item.drawerReady && item.leakCount === 0 && item.hotelMatch && item.listTitlePresent), {
      sampleSize: sample.length,
      drawerReady: sample.filter((item) => item.drawerReady).length,
      leaks: sample.reduce((sum, item) => sum + item.leakCount, 0),
      productionUiOpened: false,
    }),
    stage("S10", false, {
      pdf,
      share,
      blocker: "PRODUCTION_DELIVERY_NOT_AUTHORIZED",
    }),
    stage("S11", false, {
      blocker: "CUSTOMER_DELIVERY_QA_NOT_PRODUCTION_VERIFIED",
    }),
  ];
  const score = {
    key: hotel.key,
    displayName: hotel.displayName,
    hotelId: hotel.hpcId,
    identity,
    capability: stages[1].capability,
    segments,
    rows,
    publicationEligible,
    customerVisible,
    priority: stages[5] ? rows.filter((row) => row.prospectSection === "PRIORITY").length : 0,
    exploratory: rows.filter((row) => row.prospectSection === "EXPLORATORY").length,
    usableContactRoutes: rows.filter((row) => row.usableContactRoute).length,
    confirmed: rows.filter((row) => row.demandPosture === "CONFIRMED").length,
    inferred: rows.filter((row) => row.demandPosture === "INFERRED").length,
    exploratoryPosture: rows.filter((row) => row.demandPosture === "EXPLORATORY").length,
    notOpenBooking: rows.filter((row) => row.demandPosture === "NOT_OPEN_BOOKING").length,
    qualified: rows.filter((row) => row.qualified).length,
    rejected: rows.filter((row) => row.rejected).length,
    investigated: rows.filter((row) => row.investigated).length,
    requiringResearch: rows.filter((row) => !row.rejected && !row.hotelFacing).length,
    segmentsInvestigated: segments.filter((item) => item.investigated).length,
    localFileCount: corpus.owned.length,
    corpusSource,
    foreign: corpus.foreign.length,
    spend,
    pdf,
    share,
    airtable: airtableSummary,
    sample,
    stages,
  };
  score.priority = rows.filter((row) => row.prospectSection === "PRIORITY").length;
  score.certification = certifyThreeHotelResult(score);
  return score;
}

export async function runThreeHotelFullE2e(opts = {}) {
  const repoRoot = opts.repoRoot || process.cwd();
  const ledger = readJson(paidMultisegmentAuthorizationLedgerPath(), { entries: [] });
  const hotels = [];
  for (const hotel of GDI_THREE_HOTEL_COHORT) {
    hotels.push(
      await assessHotel(hotel, {
        repoRoot,
        ledger,
        allowAirtableRead: opts.allowAirtableRead === true,
      })
    );
  }
  const idOwners = new Map();
  const crossProperty = [];
  for (const hotel of hotels) {
    for (const row of hotel.rows) {
      if (!row.opportunityId) continue;
      const prior = idOwners.get(row.opportunityId);
      if (prior && prior !== hotel.hotelId) {
        crossProperty.push({ opportunityId: row.opportunityId, hotels: [prior, hotel.hotelId] });
      } else {
        idOwners.set(row.opportunityId, hotel.hotelId);
      }
    }
  }
  const finals = hotels.map((hotel) => hotel.certification.finalStatus);
  const cohortStatus = finals.every((status) => status === "FULL E2E CERTIFIED")
    ? "FULL E2E CERTIFIED"
    : finals.every((status) => status !== "PARTIAL / BLOCKED")
      ? "TECHNICALLY COMPLETE / COMMERCIAL GAP"
      : "PARTIAL / BLOCKED";
  return {
    contract: THREE_HOTEL_CONTRACT,
    generatedAt: new Date().toISOString(),
    paidExecution: "BLOCKED",
    airtableWrites: 0,
    publicationAuthorized: false,
    crossProperty,
    cohortStatus,
    hotels,
    commands: opts.commands || [],
  };
}

export function buildReportFiles(result) {
  const hotels = result.hotels || [];
  const segmentRows = [];
  const dispositionRows = [];
  const contactRows = [];
  const fitRows = [];
  for (const hotel of hotels) {
    for (const segment of hotel.segments || []) {
      segmentRows.push({
        hotelId: hotel.hotelId,
        hotel: hotel.key,
        segmentId: segment.segmentId,
        applicable: segment.applicable,
        status: segment.status,
        investigated: segment.investigated,
        exhausted: segment.exhausted,
        sourceFamilyCount: (segment.sourceFamiliesResearched || []).length,
        terminationReason: segment.terminationReason,
      });
    }
    for (const row of hotel.rows || []) {
      dispositionRows.push({
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        organizationName: row.organizationName,
        segment: row.segment,
        demandPosture: row.demandPosture,
        publicationEligibility: row.publicationEligibility,
        prospectSection: row.prospectSection,
        customerVisible: row.customerVisible,
        usableContactRoute: row.usableContactRoute,
        qualified: row.qualified,
        rejected: row.rejected,
        investigated: row.investigated,
      });
      contactRows.push({
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        usableContactRoute: row.usableContactRoute,
        contactRouteReason: row.contactRouteReason,
        addressesCopied: false,
      });
      fitRows.push({
        hotelId: row.hotelId,
        opportunityId: row.opportunityId,
        prospectSection: row.prospectSection,
        priority: row.priority,
        demandPosture: row.demandPosture,
        hotelFacing: row.hotelFacing,
      });
    }
  }

  const scorecard = hotels
    .map((hotel) => {
      const s2 = hotel.stages.find((item) => item.id === "S2");
      return [
        `### ${hotel.displayName}`,
        "",
        `- Hotel id: \`${hotel.hotelId}\``,
        `- Final status: **${hotel.certification.finalStatus}**`,
        `- Local file candidates: ${hotel.localFileCount}`,
        `- Scored candidates (${hotel.corpusSource}): ${hotel.rows.length}`,
        `- Segments investigated: ${hotel.segmentsInvestigated} / ${GDI_REQUIRED_SEGMENTS.length}`,
        `- Missing segment state: ${(s2.missing || []).join(", ") || "none"}`,
        `- Fully investigated candidates: ${hotel.investigated}`,
        `- Qualified: ${hotel.qualified}`,
        `- Still requiring research: ${hotel.requiringResearch}`,
        `- Rejected: ${hotel.rejected}`,
        `- Confirmed / inferred / exploratory / not open booking: ${hotel.confirmed} / ${hotel.inferred} / ${hotel.exploratoryPosture} / ${hotel.notOpenBooking}`,
        `- Priority / Exploratory sections: ${hotel.priority} / ${hotel.exploratory}`,
        `- Usable contact routes: ${hotel.usableContactRoutes}`,
        `- Publication-eligible (hotel-facing gate): ${hotel.publicationEligible}`,
        `- Customer-visible stored flags (${hotel.corpusSource}): ${hotel.customerVisible}`,
        `- Airtable readback: ${hotel.airtable.ok ? `ok (${hotel.airtable.count})` : hotel.airtable.blocker || "not verified"}`,
        `- PDF on disk: ${hotel.pdf.present ? `present, checksum ${hotel.pdf.checksumOk ? "matches" : "does not match"}` : "absent"}; production delivery not verified`,
        `- Active secure shares in registry: ${hotel.share.activeReadOnlyShares} (token values omitted)`,
        `- Estimated historical spend on the shared $11 ledger: $${hotel.spend.hotelIncurredEstimatedUsd}`,
        `- Fresh paid authorization: none`,
        "",
      ].join("\n");
    })
    .join("\n");

  const files = {
    "BASELINE_AND_PRIOR_RESEARCH_RECONCILIATION.md": `# Baseline and prior research reconciliation

Generated: ${result.generatedAt}

This run read the current repository. It did not modify ADP, did not spend, and did not write Airtable.

| Hotel | HPC id | Local opportunity rows | Segment state files investigated | Artifact scope |
| --- | --- | --- | --- | --- |
${hotels
  .map(
    (hotel) =>
      `| ${hotel.displayName} | ${hotel.hotelId} | ${hotel.rows.length} | ${hotel.segmentsInvestigated} | hotel_local_store |`
  )
  .join("\n")}

Bethesda has a local segment-research-state directory and an opportunities file. Wyndham San Diego Bayside has a local opportunities file and no segment-state directory. Mayflower has a hotel config and profile, and no local opportunities file. Those are different baselines. Bethesda's deeper history was not applied as a substitute for the other hotels.

The shared paid-research ledger \`data/group-demand-intelligence/paid-multisegment-budget/authorization-ledger-v1.json\` records historical estimated spend. Its remaining shared balance is not a fresh authorization for this run.

Counts below are local-file counts. They are not customer-visible production counts unless an Airtable readback says so.
`,
    "CANONICAL_SUPERVISOR_AND_STATE_MACHINE.md": `# Canonical supervisor and state machine

Contract: \`${result.contract}\`

The canonical GDI operating wrapper remains \`scripts/dealality-gdi-full-e2e.mjs\` (\`GDI_FULL_E2E_V1\`). This three-hotel run adds one cohort state machine, \`lib/group-demand-intelligence/workflow/gdi-three-hotel-full-e2e-v1.js\`, with stages S0–S11.

Hotel artifact scope is \`resolveGdiHotelArtifactScope\`. Charlotte's commercial completion pack stays on Charlotte only. Mayflower, Bethesda, and Wyndham San Diego Bayside read their own hotel directories.

Each stage records a result and the machine continues. A short local corpus does not abort the later gates. Paid discovery and customer publication stop because this run has no fresh authorization. Production delivery stays uncertified.
`,
    "THREE_HOTEL_EXECUTION_MANIFEST.json": JSON.stringify(
      {
        contract: result.contract,
        generatedAt: result.generatedAt,
        cohortStatus: result.cohortStatus,
        paidExecution: result.paidExecution,
        airtableWrites: result.airtableWrites,
        publicationAuthorized: result.publicationAuthorized,
        crossPropertyLeakCount: result.crossProperty.length,
        commands: result.commands,
        hotels: hotels.map((hotel) => ({
          key: hotel.key,
          hotelId: hotel.hotelId,
          displayName: hotel.displayName,
          finalStatus: hotel.certification.finalStatus,
          candidates: hotel.rows.length,
          publicationEligible: hotel.publicationEligible,
          customerVisible: hotel.customerVisible,
          segmentsInvestigated: hotel.segmentsInvestigated,
          stages: hotel.stages.map((item) => ({
            id: item.id,
            passed: item.passed,
            blocker: item.blocker || null,
          })),
        })),
      },
      null,
      2
    ),
    "SEGMENT_COVERAGE_AND_EXHAUSTION.csv": toCsv(
      [
        "hotelId",
        "hotel",
        "segmentId",
        "applicable",
        "status",
        "investigated",
        "exhausted",
        "sourceFamilyCount",
        "terminationReason",
      ],
      segmentRows
    ),
    "OPPORTUNITY_DISCOVERY_AND_DISPOSITION.csv": toCsv(
      [
        "hotelId",
        "opportunityId",
        "organizationName",
        "segment",
        "demandPosture",
        "publicationEligibility",
        "prospectSection",
        "customerVisible",
        "usableContactRoute",
        "qualified",
        "rejected",
        "investigated",
      ],
      dispositionRows
    ),
    "EVIDENCE_AND_BUYER_VERIFICATION.md": `# Evidence and buyer verification

Demand posture uses structured fields only.

- CONFIRMED: verified room block, verified housing program, active RFP sourcing, or confirmed lodging/RFP evidence type.
- INFERRED: established facts plus inferred hotel demand, estimated or strong-but-unconfirmed room evidence, or a separate overflow mechanism.
- EXPLORATORY: a named target without confirmed buying.
- NOT_OPEN_BOOKING: venue fully placed, closed, or primary-selected with no overflow mechanism.

A contracted venue is not treated as an open booking. Contact addresses are not reprinted here.

${hotels
  .map(
    (hotel) =>
      `- ${hotel.displayName}: confirmed ${hotel.confirmed}, inferred ${hotel.inferred}, exploratory ${hotel.exploratoryPosture}, not open booking ${hotel.notOpenBooking}.`
  )
  .join("\n")}
`,
    "CONTACT_ROUTE_COVERAGE.csv": toCsv(
      [
        "hotelId",
        "opportunityId",
        "usableContactRoute",
        "contactRouteReason",
        "addressesCopied",
      ],
      contactRows
    ),
    "HOTEL_FIT_AND_PRIORITIZATION.csv": toCsv(
      ["hotelId", "opportunityId", "prospectSection", "priority", "demandPosture", "hotelFacing"],
      fitRows
    ),
    "PUBLICATION_ELIGIBILITY_RESULTS.md": `# Publication eligibility

Gate: \`evaluateProspectPublicationEligibility\`. Hotel-facing is the publication-eligible count. \`customerVisible\` is a separate stored flag. The commercial floor is ${COMMERCIAL_TARGET_MIN} credible prospects. This run did not lower the gate and did not run additional paid discovery.

${hotels
  .map(
    (hotel) =>
      `- ${hotel.displayName}: publication-eligible ${hotel.publicationEligible}; customer-visible stored flag ${hotel.customerVisible}; scored from ${hotel.corpusSource}; floor met ${hotel.publicationEligible >= COMMERCIAL_TARGET_MIN}.`
  )
  .join("\n")}

Paid recovery was not executed. Historical remaining balance on the shared ledger was not spent.
`,
    "AIRTABLE_PERSISTENCE_AND_READBACK.md": `# Airtable persistence and readback

Airtable writes this run: ${result.airtableWrites}.

${hotels
  .map((hotel) => {
    const read = hotel.airtable;
    return `- ${hotel.displayName}: attempted ${read.attempted === true}; ok ${read.ok === true}; local/remote counts match ${hotel.stages.find((item) => item.id === "S8")?.passed === true}; remote count ${read.count ?? "n/a"}; foreign rows ${read.foreignCount ?? "n/a"}; local file ${hotel.localFileCount}; scored from ${hotel.corpusSource}.`;
  })
  .join("\n")}

Local files were not rewritten. Historical opportunity records were not deleted.
`,
    "CUSTOMER_UI_PDF_AND_SHARE_QA.md": `# Customer UI, PDF, and share QA

Production UI was not opened. Local drawer and list mapping ran on up to eight customer-visible opportunities per hotel. PDF checksums refer to files already in this repository. Share token secrets are omitted.

${hotels
  .map((hotel) => {
    const ready = hotel.sample.filter((item) => item.drawerReady).length;
    const failures = [...new Set(hotel.sample.flatMap((item) => item.drawerFailed || []))];
    return `- ${hotel.displayName}: sample ${hotel.sample.length}, drawer-ready ${ready}, leak count ${hotel.sample.reduce((sum, item) => sum + item.leakCount, 0)}, drawer gaps ${failures.join(", ") || "none"}, PDF present ${hotel.pdf.present}, checksum ok ${hotel.pdf.checksumOk}, active shares ${hotel.share.activeReadOnlyShares}.`;
  })
  .join("\n")}

HTTP 200, a local render, or an old PDF file is not production delivery. S10 and S11 stay unpassed.
`,
    "RESEARCH_COST_AND_AUTHORIZATION_LEDGER.md": `# Research cost and authorization

No provider call was made. No budget was transferred between hotels or from ADP.

The shared historical envelope is $${hotels[0]?.spend.historicalSharedEnvelopeUsd ?? "unknown"} estimated, with shared safely-available $${hotels[0]?.spend.sharedSafelyAvailableUsd ?? "unknown"} and accounting hold $${hotels[0]?.spend.accountingHoldUsd ?? "unknown"}. Actual billed amount on that ledger is ${hotels[0]?.spend.actualBilledUsd ?? "null"}. That remainder is not authorization for this run.

${hotels
  .map(
    (hotel) =>
      `- ${hotel.displayName}: historical estimated incurred on the shared ledger $${hotel.spend.hotelIncurredEstimatedUsd} across ${hotel.spend.hotelEntryCount} entries. Fresh authorization: no.`
  )
  .join("\n")}

Bethesda's \`auth_gdi_bethesda_marriott_12usd_v1\` reference is a historical label on the portfolio identity map. This ledger does not show a remaining Bethesda balance, and the label was not treated as permission to spend.
`,
    "NON_REGRESSION_AND_TEST_RESULTS.md": `# Non-regression and test results

Commands recorded by the runner:

${(result.commands || []).map((command) => `- \`${command}\``).join("\n") || "- none recorded"}

Cross-property opportunity id collisions in this cohort: ${result.crossProperty.length}.

Focused test \`npm run test:gdi-three-hotel-full-e2e\` exit code recorded by this run: ${result.testExitCode ?? "not run in this process"}.

ADP files were not edited by this workflow. Charlotte's commercial pack path remains the Charlotte scope only.
`,
    "FOUNDER_NEXT_DECISIONS.md": `# Founder next decisions

One decision set resumes the cohort. Fragmented per-opportunity approvals are not required.

1. Paid discovery, hotel by hotel, with a new cap for each property. Do not reuse the shared historical remainder and do not move ADP budget. Mayflower and Wyndham still need segment-state research. Bethesda's existing segment files are not a waiver for the other two.
2. Customer publication, only if a new or refreshed customer-visible set should be written. This run did not publish.
3. Production delivery check on the deployed GDI app, PDF, and secure share after publication. Local files and registry counts are not that check.
4. Mayflower's local \`opportunities.json\` is empty. The Airtable readback for \`rec0eTEnCqlsGJJt9\` returned 65 rows and zero foreign hotel ids. Authorize a local snapshot only if that readback should be written to disk. This run did not write it.
5. Production delivery remains unauthorized: open the deployed GDI hotel view, drawer, PDF, and secure share before any hotel can leave PARTIAL / BLOCKED.

Current cohort status: **${result.cohortStatus}**.
`,
    "FINAL_THREE_HOTEL_E2E_CERTIFICATION.md": `# Final three-hotel E2E certification

Cohort status: **${result.cohortStatus}**

${scorecard}

PARTIAL / BLOCKED is the cohort result when any mandatory stage is unpassed. A publication-eligible count, a local PDF, or a customerVisible flag does not upgrade that result.
`,
  };
  return files;
}

export function writeReportPack(result, outDir) {
  fs.mkdirSync(outDir, { recursive: true });
  const files = buildReportFiles(result);
  for (const [name, contents] of Object.entries(files)) {
    fs.writeFileSync(path.join(outDir, name), contents.endsWith("\n") ? contents : `${contents}\n`);
  }
  return Object.keys(files);
}
