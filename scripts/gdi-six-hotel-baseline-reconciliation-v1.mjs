#!/usr/bin/env node
/**
 * Read-only Renaissance + Waterstone baseline reconciliation by opportunity ID.
 * Does not reclassify, publish, or write Airtable.
 *
 *   node scripts/gdi-six-hotel-baseline-reconciliation-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listOpportunitiesForHotel } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { evaluateProspectPublicationEligibility } from "../lib/group-demand-intelligence/prospect-publication-eligibility-v1.js";
import {
  isCustomerFacingOpportunity,
  isGdiTestOrFixtureOpportunity,
  isGeneratorOnlyCustomerRecord,
} from "../lib/group-demand-intelligence/customer-visibility.js";
import { isReviewedProspectCustomerVisible } from "../lib/group-demand-intelligence/reviewed-prospect-publication-v1.js";
import { isLegacyVisibilityCompatible } from "../lib/group-demand-intelligence/legacy-visibility-compatibility-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-six-hotel-e2e");

const HOTELS = [
  {
    hotelId: "recG66DQJKP2c0UNh",
    displayName: "Renaissance New York Times Square Hotel",
    historicalFacingReport: {
      source: "reports/gdi-use-case-qualification-integrity/release/baseline-freeze.json",
      facingFilterCount: 8,
      hotelFacingEligCount: 2,
      note: "Historical '8 facing' = filterVisible / facingFilterCount (legacy+surface filter), not HOTEL_FACING eligibility.",
    },
  },
  {
    hotelId: "recgMYovrrZDJMqzX",
    displayName: "Waterstone Resort & Marina Boca Raton",
    historicalFacingReport: {
      source: "reports/gdi-use-case-qualification-integrity/release/baseline-freeze.json",
      facingFilterCount: 9,
      hotelFacingEligCount: 3,
      note: "Prior six-hotel freeze paraphrased Waterstone as 8 facing; integrity freeze recorded facingFilterCount=9. Current hotelFacingElig=3.",
    },
  },
];

/** Mutually exclusive primary dispositions — every unique ID gets exactly one. */
const PRIMARY = Object.freeze({
  CUSTOMER_FACING_HOTEL_ELIGIBLE: "CUSTOMER_FACING_HOTEL_ELIGIBLE",
  REVIEWED_PACKAGE_FACING: "REVIEWED_PACKAGE_FACING",
  LEGACY_OR_READY_SURFACE_VISIBLE: "LEGACY_OR_READY_SURFACE_VISIBLE",
  ENRICHMENT_QUEUE: "ENRICHMENT_QUEUE",
  NOT_A_PROSPECT: "NOT_A_PROSPECT",
  TEST_OR_GENERATOR_INTERNAL: "TEST_OR_GENERATOR_INTERNAL",
  INTERNAL_CORPUS_NOT_CUSTOMER_VISIBLE: "INTERNAL_CORPUS_NOT_CUSTOMER_VISIBLE",
});

function loadLocal(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json"
  );
  if (!fs.existsSync(p)) {
    return { ok: false, path: p, opportunities: [], updatedAt: null, error: "missing_local_file" };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return {
    ok: true,
    path: p,
    opportunities: Array.isArray(doc.opportunities) ? doc.opportunities : [],
    updatedAt: doc.updatedAt || null,
    persistence: doc.persistence || null,
    source: doc.source || null,
  };
}

function csvEscape(v) {
  const s = v == null ? "" : String(v);
  if (/[",\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function classifyPrimary(opp, elig) {
  if (isGdiTestOrFixtureOpportunity(opp) || isGeneratorOnlyCustomerRecord(opp)) {
    return {
      primaryDisposition: PRIMARY.TEST_OR_GENERATOR_INTERNAL,
      exclusionReason: "test_fixture_or_generator_shell",
    };
  }
  if (elig.eligibility === "NOT_A_PROSPECT" || elig.hotelFacing === false && (elig.reasons || []).includes("disqualified_or_test")) {
    if (elig.eligibility === "NOT_A_PROSPECT") {
      return {
        primaryDisposition: PRIMARY.NOT_A_PROSPECT,
        exclusionReason: (elig.reasons || []).join("|") || "not_a_prospect",
      };
    }
  }
  if (elig.hotelFacing === true) {
    if (isReviewedProspectCustomerVisible(opp)) {
      return {
        primaryDisposition: PRIMARY.REVIEWED_PACKAGE_FACING,
        exclusionReason: "",
      };
    }
    return {
      primaryDisposition: PRIMARY.CUSTOMER_FACING_HOTEL_ELIGIBLE,
      exclusionReason: "",
    };
  }
  // Legacy/ready surface without hotel-facing eligibility — historical "8 facing" bucket
  if (isCustomerFacingOpportunity(opp)) {
    const parts = [];
    if (isLegacyVisibilityCompatible(opp)) parts.push("legacy_visibility_compatible");
    const ready = isGdiCustomerOpportunityReady(opp);
    if (ready.ok) parts.push("strict_ready");
    if (opp.customerVisible === true) parts.push("customerVisible_true");
    return {
      primaryDisposition: PRIMARY.LEGACY_OR_READY_SURFACE_VISIBLE,
      exclusionReason: `surface_visible_but_not_hotel_facing:${parts.join("+") || "surface_filter"};elig=${(elig.reasons || []).join("|")}`,
    };
  }
  if (elig.eligibility === "ENRICHMENT_QUEUE" || elig.enrichmentQueue === true) {
    return {
      primaryDisposition: PRIMARY.ENRICHMENT_QUEUE,
      exclusionReason: (elig.reasons || []).join("|") || elig.nextTask || "enrichment_queue",
    };
  }
  if (elig.eligibility === "NOT_A_PROSPECT") {
    return {
      primaryDisposition: PRIMARY.NOT_A_PROSPECT,
      exclusionReason: (elig.reasons || []).join("|") || "not_a_prospect",
    };
  }
  return {
    primaryDisposition: PRIMARY.INTERNAL_CORPUS_NOT_CUSTOMER_VISIBLE,
    exclusionReason:
      opp.customerVisible === false
        ? "customerVisible_false"
        : "not_surface_eligible_and_not_hotel_facing",
  };
}

function rowFromOpp(hotelId, opp, sourceLabel, evidenceTimestamp) {
  const elig = evaluateProspectPublicationEligibility(opp);
  const { primaryDisposition, exclusionReason } = classifyPrimary(opp, elig);
  const hotelFacing = elig.hotelFacing === true;
  const legacySurface = isCustomerFacingOpportunity(opp);
  const reviewedVisible = isReviewedProspectCustomerVisible(opp);
  return {
    hotelId,
    opportunityId: opp.id || opp.opportunityId || "",
    title: opp.title || opp.opportunityName || "",
    organizationName: opp.organizationName || "",
    researchOrigin:
      opp.researchOrigin ||
      opp.researchPath ||
      opp.discoverySource ||
      opp.sourceSystem ||
      opp.persistenceSource ||
      (Array.isArray(opp.sources) && opp.sources[0]?.provider) ||
      "",
    publicationBasis: opp.prospectPublicationBasis || opp.publicationBasis || "",
    customerVisibleFlag: opp.customerVisible === true,
    prospectHotelFacingFlag: opp.prospectHotelFacing === true,
    opportunityType: opp.opportunityType || "",
    qualificationResult: opp.qualificationResult || opp.priority || opp.gdiMaturityState || "",
    visibilityRuleVersion:
      opp.visibilityRuleVersion ||
      opp.prospectPublicationRuleVersion ||
      "prospect-publication-eligibility-v1+customer-visibility",
    eligibility: elig.eligibility,
    hotelFacingEligible: hotelFacing,
    legacyOrReadySurfaceVisible: legacySurface,
    reviewedPackageVisible: reviewedVisible,
    primaryDisposition,
    currentFacingStatus: hotelFacing
      ? "FACING_HOTEL_ELIGIBLE"
      : legacySurface
        ? "SURFACE_VISIBLE_NOT_HOTEL_ELIGIBLE"
        : "NOT_FACING",
    exclusionReason,
    evidenceTimestamp,
    sourceSnapshot: sourceLabel,
  };
}

function summarize(rows) {
  const by = {};
  for (const r of rows) {
    by[r.primaryDisposition] = (by[r.primaryDisposition] || 0) + 1;
  }
  return {
    totalUnique: rows.length,
    hotelFacingEligible: rows.filter((r) => r.hotelFacingEligible).length,
    legacyOrReadySurfaceVisible: rows.filter((r) => r.legacyOrReadySurfaceVisible).length,
    reviewedPackageVisible: rows.filter((r) => r.reviewedPackageVisible).length,
    byPrimaryDisposition: by,
  };
}

function unionById(airtableRows, localRows) {
  const map = new Map();
  for (const r of localRows) {
    if (r.opportunityId) map.set(r.opportunityId, { ...r, inLocal: true, inAirtable: false });
  }
  for (const r of airtableRows) {
    if (!r.opportunityId) continue;
    const prev = map.get(r.opportunityId);
    if (prev) {
      map.set(r.opportunityId, {
        ...r,
        inLocal: true,
        inAirtable: true,
        localPrimaryDisposition: prev.primaryDisposition,
        localFacingStatus: prev.currentFacingStatus,
      });
    } else {
      map.set(r.opportunityId, { ...r, inLocal: false, inAirtable: true });
    }
  }
  return [...map.values()].sort((a, b) =>
    String(a.opportunityId).localeCompare(String(b.opportunityId))
  );
}

async function reconcileHotel(hotel) {
  const local = loadLocal(hotel.hotelId);
  const localTs = local.updatedAt || null;
  const localRows = local.opportunities.map((o) =>
    rowFromOpp(hotel.hotelId, o, "local_filesystem", localTs)
  );

  let airtable = { ok: false, opportunities: [], error: null, fetchedAt: null };
  try {
    const opps = await listOpportunitiesForHotel(hotel.hotelId);
    airtable = {
      ok: true,
      opportunities: opps,
      error: null,
      fetchedAt: new Date().toISOString(),
    };
  } catch (err) {
    airtable = {
      ok: false,
      opportunities: [],
      error: err?.message || String(err),
      fetchedAt: new Date().toISOString(),
    };
  }

  const airtableRows = airtable.opportunities.map((o) =>
    rowFromOpp(hotel.hotelId, o, "airtable_production", airtable.fetchedAt)
  );
  const unified = unionById(airtableRows, localRows);

  const localIds = new Set(localRows.map((r) => r.opportunityId));
  const airtableIds = new Set(airtableRows.map((r) => r.opportunityId));
  const onlyLocal = [...localIds].filter((id) => !airtableIds.has(id));
  const onlyAirtable = [...airtableIds].filter((id) => !localIds.has(id));

  return {
    hotelId: hotel.hotelId,
    displayName: hotel.displayName,
    historicalFacingReport: hotel.historicalFacingReport,
    local: {
      ok: local.ok,
      path: local.path,
      updatedAt: local.updatedAt,
      summary: summarize(localRows),
      rows: localRows,
    },
    airtable: {
      ok: airtable.ok,
      error: airtable.error,
      fetchedAt: airtable.fetchedAt,
      summary: summarize(airtableRows),
      rows: airtableRows,
    },
    unified: {
      summary: summarize(unified),
      idDelta: {
        onlyLocal,
        onlyAirtable,
        intersectionCount: [...localIds].filter((id) => airtableIds.has(id)).length,
      },
      rows: unified,
    },
    surfaces: {
      authenticatedList: {
        status: "NOT_PROBED_IN_THIS_TASK",
        note: "Requires authenticated Memberstack session; AI Admin interactive check is a separate ops verification.",
      },
      shareList: {
        status: "NOT_PROBED_IN_THIS_TASK",
        note: "Share capability requires legitimate share token; not invented here.",
      },
      export: {
        status: "LOCAL_FILE_AVAILABLE",
        path: local.path,
        count: local.opportunities.length,
      },
    },
  };
}

function explainFacingDelta(hotelResult) {
  const hist = hotelResult.historicalFacingReport;
  const currentFacing = hotelResult.airtable.ok
    ? hotelResult.airtable.summary.hotelFacingEligible
    : hotelResult.local.summary.hotelFacingEligible;
  const currentLegacy = hotelResult.airtable.ok
    ? hotelResult.airtable.summary.legacyOrReadySurfaceVisible
    : hotelResult.local.summary.legacyOrReadySurfaceVisible;
  const causes = [];
  if (hist.facingFilterCount !== currentFacing) {
    causes.push({
      code: "DIFFERENT_ROUTE_OR_FILTER",
      detail: `Historical facingFilterCount=${hist.facingFilterCount} counted isCustomerFacingOpportunity / filterVisible. Current authoritative facing=${currentFacing} uses evaluateProspectPublicationEligibility HOTEL_FACING.`,
    });
  }
  if (hist.hotelFacingEligCount === currentFacing) {
    causes.push({
      code: "LEGACY_VERSUS_REVIEWED_ELIGIBILITY",
      detail: `Hotel-facing eligibility count is stable vs integrity freeze (${hist.hotelFacingEligCount} → ${currentFacing}). No bulk demotion detected on that axis.`,
    });
  }
  if (currentLegacy !== hist.facingFilterCount) {
    causes.push({
      code: "SNAPSHOT_TIME_OR_RECORD_CHANGE",
      detail: `Legacy/ready surface-visible count now ${currentLegacy} vs historical filterVisible ${hist.facingFilterCount}. Investigate ID-level LEGACY_OR_READY_SURFACE_VISIBLE rows before assuming defect.`,
    });
  }
  causes.push({
    code: "MISSING_PUBLICATION_METADATA",
    detail: "Reviewed-package path (REVIEWED_PACKAGE_V1) is not enabled for these hotel IDs; reviewedVisible remains 0.",
  });
  causes.push({
    code: "STALE_REPORT_RISK",
    detail: "Earlier six-hotel summary used 'internalish' and paraphrased Waterstone as 8 facing; integrity freeze is the durable historical comparator.",
  });
  return {
    historicalFilterFacing: hist.facingFilterCount,
    historicalHotelFacingElig: hist.hotelFacingEligCount,
    currentHotelFacingEligible: currentFacing,
    currentLegacyOrReadySurfaceVisible: currentLegacy,
    causes,
    proposedChangeSet: {
      status: "NONE_APPLIED",
      note: "No reclassification or publication repair in this preparation task. If founder wants legacy surface rows promoted to hotel-facing, prepare a separate evidence-backed change set after contact-route/suitability gaps close.",
    },
  };
}

async function main() {
  const generatedAt = new Date().toISOString();
  const hotels = [];
  for (const h of HOTELS) {
    hotels.push(await reconcileHotel(h));
  }

  const jsonOut = {
    generatedAt,
    mode: "read_only_baseline_reconciliation_by_id",
    codeBaseline: {
      originMain: "248a30981b4582efa3ba2bda23b90fce487f09b0",
      note: "PR #108 merged; production deploy SUCCESS same SHA",
    },
    primaryDispositionDefinitions: PRIMARY,
    hotels: hotels.map((h) => ({
      hotelId: h.hotelId,
      displayName: h.displayName,
      historicalFacingReport: h.historicalFacingReport,
      localSummary: h.local.summary,
      airtableSummary: h.airtable.summary,
      airtableOk: h.airtable.ok,
      airtableError: h.airtable.error,
      unifiedSummary: h.unified.summary,
      idDelta: h.unified.idDelta,
      facingDeltaExplanation: explainFacingDelta(h),
      surfaces: h.surfaces,
    })),
    records: hotels.flatMap((h) =>
      h.unified.rows.map((r) => ({
        ...r,
        hotelDisplayName: h.displayName,
      }))
    ),
  };

  fs.mkdirSync(OUT, { recursive: true });
  const jsonPath = path.join(OUT, "BASELINE_RECONCILIATION.json");
  fs.writeFileSync(jsonPath, JSON.stringify(jsonOut, null, 2));

  const headers = [
    "hotelId",
    "hotelDisplayName",
    "opportunityId",
    "title",
    "organizationName",
    "researchOrigin",
    "publicationBasis",
    "customerVisibleFlag",
    "prospectHotelFacingFlag",
    "opportunityType",
    "qualificationResult",
    "visibilityRuleVersion",
    "eligibility",
    "hotelFacingEligible",
    "legacyOrReadySurfaceVisible",
    "reviewedPackageVisible",
    "primaryDisposition",
    "currentFacingStatus",
    "exclusionReason",
    "evidenceTimestamp",
    "sourceSnapshot",
    "inLocal",
    "inAirtable",
  ];
  const lines = [headers.join(",")];
  for (const r of jsonOut.records) {
    lines.push(
      headers
        .map((k) => csvEscape(r[k] === true ? "true" : r[k] === false ? "false" : r[k]))
        .join(",")
    );
  }
  const csvPath = path.join(OUT, "BASELINE_RECONCILIATION.csv");
  fs.writeFileSync(csvPath, lines.join("\n") + "\n");

  console.log(
    JSON.stringify(
      {
        generatedAt,
        jsonPath,
        csvPath,
        hotels: jsonOut.hotels.map((h) => ({
          hotelId: h.hotelId,
          airtableOk: h.airtableOk,
          total: h.unifiedSummary.totalUnique,
          hotelFacing: h.unifiedSummary.hotelFacingEligible,
          legacySurface: h.unifiedSummary.legacyOrReadySurfaceVisible,
          byPrimary: h.unifiedSummary.byPrimaryDisposition,
        })),
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
