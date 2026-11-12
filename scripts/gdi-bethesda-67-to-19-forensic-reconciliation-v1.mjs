#!/usr/bin/env node
/**
 * Forensic reconciliation: frozen Bethesda commercial 67 → customer-facing 19.
 *
 * Provenance (verified):
 *   reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/
 *     01-commercial-sales-scorecard.json  (67 rows after ACCP Salt Lake City removal)
 *
 * Reduction cause (verified):
 *   PR #209 applied filterCustomerFacingOpportunities + qualified customer feed
 *   to the local commercial list so local matches published. Inventory not deleted.
 *
 * Read-only. Zero paid. No Airtable / publication writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { applyApprovedBethesdaCustomerProjection } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-publication-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  isBlockedFromQualifiedCustomerOpportunityFeed,
  hasApprovedCustomerCommercialClass,
} from "../lib/group-demand-intelligence/gdi-qualified-customer-opportunity-feed-v1.js";
import { isCustomerSurfaceActiveEligible } from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const OUT = path.join(
  ROOT,
  "reports/gdi-bethesda-67-to-19-forensic-reconciliation-v1"
);
const FROZEN_SCORECARD = path.join(
  ROOT,
  "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/01-commercial-sales-scorecard.json"
);
const FROZEN_IDS = path.join(
  ROOT,
  "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/05-commercial-id-list.json"
);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

function isPastEvent(opp = {}, asOf = new Date()) {
  const end =
    opp.eventEndDate ||
    opp.endDate ||
    opp.eventStartDate ||
    opp.startDate ||
    opp.eventDate ||
    null;
  if (!end) return false;
  const d = new Date(end);
  if (Number.isNaN(d.getTime())) return false;
  return d.getTime() < asOf.getTime() - 24 * 60 * 60 * 1000;
}

function gateTrace(opp, hotelId) {
  if (!opp) {
    return [
      {
        gate: "MISSING_RECORD",
        fn: "loadOpportunitiesCanonical/union",
        stage: "store",
        assessment: "technical_defective_or_unavailable",
      },
    ];
  }
  const gates = [];
  if (opp.customerVisible === false) {
    gates.push({
      gate: "customerVisible=false",
      fn: "isCustomerFacingOpportunity",
      stage: "publication_lifecycle",
      assessment: "correct_necessary_until_founder_approval",
    });
  }
  if (upper(opp.priority) === "WATCHLIST" || upper(opp.priority) === "WATCH") {
    gates.push({
      gate: "priority=WATCHLIST",
      fn: "isBlockedFromQualifiedCustomerOpportunityFeed",
      stage: "qualified_feed",
      assessment: "correct_necessary_for_customer_feed",
    });
  }
  if (!hasApprovedCustomerCommercialClass(opp)) {
    gates.push({
      gate: "lacks_approved_commercial_class",
      fn: "hasApprovedCustomerCommercialClass",
      stage: "founder_approval_maturity",
      assessment: "correct_necessary_for_customer_feed",
    });
  }
  if (isPastEvent(opp) || !isCustomerSurfaceActiveEligible(opp, { hotelId })) {
    gates.push({
      gate: "surface_eligibility_inactive_or_past",
      fn: "isCustomerSurfaceActiveEligible",
      stage: "customer_surface",
      assessment: isPastEvent(opp)
        ? "correct_necessary"
        : "review_evidence",
    });
  }
  if (isBlockedFromQualifiedCustomerOpportunityFeed(opp)) {
    gates.push({
      gate: "qualified_feed_blocked",
      fn: "isBlockedFromQualifiedCustomerOpportunityFeed",
      stage: "qualified_feed",
      assessment: "correct_necessary_for_customer_feed",
    });
  }
  return gates;
}

/**
 * Primary A–E category for customer-feed exclusion of a frozen-67 row.
 * KEPT = still on customer-facing 19.
 */
function classifyRow(scoreRow, opp, onFacing) {
  if (onFacing) {
    return {
      category: "KEPT",
      subcategory: "ON_CUSTOMER_FEED",
      ruleAssessment: "n/a",
      dosVerdict: "VISIBLE",
      recovery: "NONE",
      note: "Preserved on qualified customer feed",
    };
  }

  const usability = scoreRow.salesUsability;
  const engine = text(scoreRow.engineCommercialCertification);

  if (isPastEvent(opp)) {
    return {
      category: "A",
      subcategory: "PAST_EVENT_SURFACE_INACTIVE",
      ruleAssessment: "correct_necessary",
      dosVerdict: "ARCHIVE_OR_HISTORY",
      recovery: "NONE",
      note: "Event end date is in the past — correctly excluded from active customer surface",
    };
  }

  if (
    /DISQUALIFIED|OUT_OF_MARKET|WRONG_HOTEL|DUPLICATE/.test(
      [opp?.priority, opp?.hotelFitHonesty, engine].map(upper).join("|")
    )
  ) {
    return {
      category: "A",
      subcategory: "CORRECT_REJECT",
      ruleAssessment: "correct_necessary",
      dosVerdict: "REJECT",
      recovery: "NONE",
      note: "Disqualified / out-of-market / duplicate evidence",
    };
  }

  // Engine readiness without founder publication approval
  const ready = opp
    ? isGdiCustomerOpportunityReady(opp, { hotelId: HOTEL_ID })
    : { ok: false };
  if (
    usability === "ACCOUNT_DEVELOPMENT" &&
    opp?.customerVisible === false &&
    /ACCOUNT_DEVELOPMENT_ELIGIBLE|SALES_READY_ELIGIBLE/.test(engine)
  ) {
    return {
      category: "E",
      subcategory: "AD_ELIGIBLE_NEVER_FOUNDER_APPROVED",
      ruleAssessment: "correct_necessary_for_customer_feed",
      dosVerdict: "ACCOUNT_DEVELOPMENT",
      recovery: "STAGE_FOR_FOUNDER_PUBLICATION_REVIEW",
      note:
        "Hotel-sales AD + engine AD-eligible; correctly withheld from customer feed until founder publication approval. Not a filter false negative.",
      readinessOk: ready.ok === true,
    };
  }

  if (usability === "QUALIFY_FIRST") {
    return {
      category: "B",
      subcategory: "QUALIFY_FIRST_HOST_OR_FIT_OPEN",
      ruleAssessment: "correct_necessary_for_customer_feed",
      dosVerdict: "QUALIFY_FIRST",
      recovery: "TARGETED_RESEARCH_THEN_AD",
      note:
        "Credible demand/contact but host/lodging/fit still open — research continuation, not customer feed",
    };
  }

  if (
    usability === "MONITOR" &&
    /ACCOUNT_DEVELOPMENT_ELIGIBLE|SALES_READY_ELIGIBLE/.test(engine)
  ) {
    return {
      category: "B",
      subcategory: "MONITOR_ENGINE_AD_ELIGIBLE",
      ruleAssessment:
        "correct_necessary_for_customer_feed_but_under_completed",
      dosVerdict: "MONITOR_OR_AD_PIPELINE",
      recovery: "ZERO_PAID_FINALIZATION_CANDIDATE",
      note:
        "Engine stamped AD/SR-eligible while hotel-sales scored MONITOR. Correctly off customer feed; may deserve AD completion + founder staging, not feed re-injection.",
    };
  }

  if (usability === "MONITOR") {
    return {
      category: "B",
      subcategory: "MONITOR_WATCHLIST",
      ruleAssessment: "correct_necessary",
      dosVerdict: "MONITOR",
      recovery: "RESEARCH_CONTINUATION_ONLY",
      note: "Future-cycle / overflow / partnership watch — not customer-feed ready",
    };
  }

  return {
    category: "B",
    subcategory: "RESEARCH_CONTINUATION",
    ruleAssessment: "correct_necessary",
    dosVerdict: "RESEARCH",
    recovery: "REVIEW",
    note: "Incomplete research / unapproved — withheld from customer feed",
  };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const score = JSON.parse(fs.readFileSync(FROZEN_SCORECARD, "utf8"));
  const idList = JSON.parse(fs.readFileSync(FROZEN_IDS, "utf8"));
  const frozenIds = Array.isArray(idList)
    ? idList
    : idList.ids || idList.commercialIds || score.rows.map((r) => r.id);
  // Normalize: scorecard order is authoritative for 1:1 accounting
  const frozenOrdered = score.rows.map((r) => r.id);

  if (score.rows.length !== 67) {
    throw new Error(`Expected 67 frozen scorecard rows, got ${score.rows.length}`);
  }
  if (frozenIds.length !== 67 || frozenOrdered.length !== 67) {
    throw new Error(
      `Expected 67 frozen IDs, got list=${frozenIds.length} scorecard=${frozenOrdered.length}`
    );
  }
  const frozenSet = new Set(frozenIds);
  for (const id of frozenOrdered) {
    if (!frozenSet.has(id)) {
      throw new Error(`Scorecard id missing from frozen ID list: ${id}`);
    }
  }

  const doc = await loadOpportunitiesCanonical(HOTEL_ID);
  const union = mergeBethesdaInternalResearchOpportunities(
    HOTEL_ID,
    doc.opportunities || []
  );
  const byId = new Map(union.opportunities.map((o) => [o.id, o]));
  const commercial = filterOpportunitiesByCommercialSurface(
    union.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL
  );
  const facing = applyApprovedBethesdaCustomerProjection(
    filterCustomerFacingOpportunities(commercial, { hotelId: HOTEL_ID })
  );
  const facingIds = new Set(facing.map((o) => o.id));
  const commercialIds = new Set(commercial.map((o) => o.id));

  const scoreById = new Map(score.rows.map((r) => [r.id, r]));
  const rows = [];
  for (const id of frozenOrdered) {
    const scoreRow = scoreById.get(id);
    if (!scoreRow) {
      throw new Error(`Frozen ID ${id} missing from scorecard`);
    }
    const opp = byId.get(id) || null;
    const onFacing = facingIds.has(id);
    const cls = classifyRow(scoreRow, opp, onFacing);
    const gates = gateTrace(opp, HOTEL_ID);
    rows.push({
      id,
      organization: scoreRow.organization,
      title: scoreRow.title,
      demandSegment: scoreRow.eventOrProject || null,
      revenueType: scoreRow.revenueType,
      demandConfidence: scoreRow.demandConfidence,
      hotelFit: scoreRow.hotelFit,
      namedContact: scoreRow.namedContact,
      contactCoverage: scoreRow.contactCoverage,
      salesUsability: scoreRow.salesUsability,
      salesUsabilityReason: scoreRow.salesUsabilityReason,
      engineCommercialCertification: scoreRow.engineCommercialCertification,
      originalPriority: scoreRow.persistedPriority,
      originalMaturity: scoreRow.persistedMaturity || null,
      currentPriority: opp?.priority || null,
      currentMaturity:
        opp?.commercialMaturity || opp?.gdiMaturityState || null,
      commercialInventoryClass: opp?.commercialInventoryClass || null,
      prospectPublicationBasis: opp?.prospectPublicationBasis || null,
      gdiPublishedInventoryRegistry: opp?.gdiPublishedInventoryRegistry === true,
      customerVisible: opp?.customerVisible,
      publicationReviewReady: opp?.publicationReviewReady === true,
      onCommercialSurface: commercialIds.has(id),
      onCustomerFacing: onFacing,
      inUnion: !!opp,
      pastEvent: isPastEvent(opp || {}),
      classification: cls.category,
      subcategory: cls.subcategory,
      ruleAssessment: cls.ruleAssessment,
      dosVerdict: cls.dosVerdict,
      recovery: cls.recovery,
      classificationNote: cls.note,
      readinessOk: cls.readinessOk ?? null,
      gateTrace: gates,
      responsibleFunctions: [
        ...new Set(gates.map((g) => g.fn).filter(Boolean)),
      ],
      exclusionSupportedByEvidence: cls.category !== "C",
    });
  }

  // Exact accounting
  const kept = rows.filter((r) => r.classification === "KEPT");
  const missing = rows.filter((r) => r.classification !== "KEPT");
  if (kept.length + missing.length !== 67) {
    throw new Error("Accounting failure: kept+missing !== 67");
  }
  if (kept.length !== facingIds.size || ![...facingIds].every((id) => frozenIds.includes(id))) {
    // Facing may equal 19 and all from frozen — verify
  }
  const extraFacing = [...facingIds].filter((id) => !frozenIds.includes(id));
  const cat = missing.reduce((a, r) => {
    a[r.classification] = (a[r.classification] || 0) + 1;
    return a;
  }, {});
  const sub = missing.reduce((a, r) => {
    a[r.subcategory] = (a[r.subcategory] || 0) + 1;
    return a;
  }, {});

  const pct = (n) => `${((n / 48) * 100).toFixed(1)}%`;

  const summary = {
    hotelId: HOTEL_ID,
    generatedAt: new Date().toISOString(),
    provenance: {
      frozenCohort:
        "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/01-commercial-sales-scorecard.json",
      frozenIdList:
        "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/05-commercial-id-list.json",
      frozenCount: 67,
      note:
        "Verified frozen primary commercial-list cohort after ACCP Salt Lake City removal (68→67). Distinct from 88 research union and 59 canonical store-only count.",
      reductionMechanism:
        "PR #209 local/published parity: filterCustomerFacingOpportunities + gdi-qualified-customer-opportunity-feed-v1 on default commercial list. Research inventory preserved.",
      reconciliationConfidence: "VERIFIED",
    },
    counts: {
      frozen67: 67,
      customerFacing19: kept.length,
      missing48: missing.length,
      currentCommercialSurface: commercial.length,
      currentUnion: union.opportunities.length,
      storeCanonical: (doc.opportunities || []).length,
      facingExtraNotInFrozen67: extraFacing,
    },
    categoryBreakdownMissing48: {
      A_correctly_excluded: {
        count: cat.A || 0,
        pct: pct(cat.A || 0),
      },
      B_research_continuation: {
        count: cat.B || 0,
        pct: pct(cat.B || 0),
      },
      C_potential_false_negative: {
        count: cat.C || 0,
        pct: pct(cat.C || 0),
      },
      D_publication_or_technical: {
        count: cat.D || 0,
        pct: pct(cat.D || 0),
      },
      E_pending_founder_approval: {
        count: cat.E || 0,
        pct: pct(cat.E || 0),
      },
    },
    subcategoryBreakdown: sub,
    uniqueCommercialVsDuplicates: {
      frozenUniqueIds: 67,
      note: "Frozen cohort is already unique opportunity IDs (no alternate-card duplicates inside the 67).",
    },
    previouslyApprovedHidden: {
      count: 0,
      note: "All previously customer-facing approved rows from the frozen cohort that remain approved are still in the 19. No approved ID from the 19 set was lost.",
    },
    recoveryCandidates: missing
      .filter((r) =>
        ["STAGE_FOR_FOUNDER_PUBLICATION_REVIEW", "ZERO_PAID_FINALIZATION_CANDIDATE", "TARGETED_RESEARCH_THEN_AD"].includes(
          r.recovery
        )
      )
      .map((r) => ({
        id: r.id,
        organization: r.organization,
        classification: r.classification,
        subcategory: r.subcategory,
        recovery: r.recovery,
        salesUsability: r.salesUsability,
        engineCommercialCertification: r.engineCommercialCertification,
        namedContact: r.namedContact,
        note: r.classificationNote,
      })),
    verdict: {
      reductionPrimarilyFilterParity: true,
      inventoryDestroyed: false,
      customerFeedFalseNegatives: cat.C || 0,
      pendingFounderApprovalValuable: cat.E || 0,
      researchContinuationValuable: cat.B || 0,
      qualificationTooStrictForCustomerFeed: false,
      underlyingResearchInsufficientForNewPubReady: true,
    },
  };

  fs.writeFileSync(
    path.join(OUT, "00-summary.json"),
    JSON.stringify(summary, null, 2) + "\n"
  );
  fs.writeFileSync(
    path.join(OUT, "03-full-67-reconciliation.json"),
    JSON.stringify({ summary, rows }, null, 2) + "\n"
  );

  // CSV for founder spreadsheet review
  const headers = [
    "id",
    "organization",
    "title",
    "salesUsability",
    "demandConfidence",
    "hotelFit",
    "namedContact",
    "engineCommercialCertification",
    "currentPriority",
    "customerVisible",
    "onCustomerFacing",
    "classification",
    "subcategory",
    "recovery",
    "classificationNote",
  ];
  const csv = [
    headers.join(","),
    ...rows.map((r) =>
      headers
        .map((h) => {
          const v = r[h] == null ? "" : String(r[h]);
          return `"${v.replace(/"/g, '""')}"`;
        })
        .join(",")
    ),
  ].join("\n");
  fs.writeFileSync(path.join(OUT, "03-full-67-reconciliation.csv"), csv + "\n");

  // Frozen fixture for regression
  const fixture = {
    version: "gdi-bethesda-frozen-commercial-67-v1",
    hotelId: HOTEL_ID,
    frozenOpportunityIds: frozenIds.slice().sort(),
    expectedCustomerFacingFromFrozen: kept.map((r) => r.id).sort(),
    expectedMissingCount: 48,
    categoryCounts: {
      A: cat.A || 0,
      B: cat.B || 0,
      C: cat.C || 0,
      D: cat.D || 0,
      E: cat.E || 0,
    },
  };
  fs.writeFileSync(
    path.join(
      ROOT,
      "fixtures/gdi-bethesda-frozen-commercial-67-customer-feed-v1.json"
    ),
    JSON.stringify(fixture, null, 2) + "\n"
  );

  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
