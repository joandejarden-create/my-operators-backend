/**
 * GDI Executive Summary V2 — audience perspectives + metric integrity.
 *
 * Reuses V1 aggregation architecture. Does not create a parallel research engine.
 * Research Intelligence perspective is server-gated (founder/admin only).
 *
 * Contract: gdi-executive-summary-v2
 * Auth: FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_V2_20261127
 */

import {
  buildGdiExecutiveSummaryV1,
  buildCommercialEngagementSection,
  buildInvestmentRecoveryPlaceholder,
  EXEC_SUMMARY_DATA_STATE,
  GDI_EXECUTIVE_SUMMARY_AUTH,
  prioritizeCustomerOpportunities as prioritizeV1,
} from "./gdi-executive-summary-v1.js";
import {
  RESEARCH_RECEIPT_AVAILABILITY,
  RESEARCH_RECEIPT_ATTRIBUTION,
  buildResearchReceiptMetric,
  GDI_RESEARCH_RECEIPT_CONTRACT_ID,
} from "./gdi-research-receipt-contract-v1.js";
import { buildGdiResearchProductivity } from "./gdi-research-productivity-v1.js";
import {
  OPPORTUNITY_TYPE_FILTER_LABEL,
  classifyOpportunityTypeFilter,
} from "../opportunity-type-filter-v1.js";
import {
  CONTACT_ACTIONABILITY_TIER,
  classifyContactActionability,
} from "../presentation/gdi-contact-actionability-v1.js";
import {
  WEEKLY_DELTA_STATE,
  weeklyDeltaSummaryCounts,
} from "../weekly-delta.js";

export const GDI_EXECUTIVE_SUMMARY_V2 = "gdi-executive-summary-v2";
export const GDI_EXECUTIVE_SUMMARY_V2_AUTH =
  "FOUNDER_GO_GDI_EXECUTIVE_SUMMARY_V2_20261127";

export const EXEC_SUMMARY_PERSPECTIVE = Object.freeze({
  EXECUTIVE_OVERVIEW: "EXECUTIVE_OVERVIEW",
  SALES_PERFORMANCE: "SALES_PERFORMANCE",
  RESEARCH_INTELLIGENCE: "RESEARCH_INTELLIGENCE",
});

export const METRIC_GRAIN = Object.freeze({
  RESEARCH_ACTIVITY: "RESEARCH_ACTIVITY",
  QUALIFIED_INVENTORY: "QUALIFIED_INVENTORY",
  COMMERCIAL_ENGAGEMENT: "COMMERCIAL_ENGAGEMENT",
});

const CONTACT_TIER_LABEL = Object.freeze({
  [CONTACT_ACTIONABILITY_TIER.SALES_READY]: "Sales Ready",
  [CONTACT_ACTIONABILITY_TIER.REACHABLE]: "Reachable",
  [CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED]:
    "Contact Research Needed",
  [CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED]:
    "Contact Identification Needed",
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function oppId(o) {
  return text(o?.id || o?.opportunityId);
}

export function resolveCanonicalDemandSegmentLabel(opp = {}) {
  const baked =
    text(opp.opportunityTypeFilterLabel) ||
    text(opp.segmentDisplayLabel) ||
    text(opp.opportunityTypeLabel);
  if (baked && !/^unspecified$/i.test(baked)) return baked;
  let cat = text(opp.opportunityTypeFilterCategory);
  if (!cat) {
    try {
      cat = classifyOpportunityTypeFilter(opp);
    } catch {
      cat = "";
    }
  }
  if (cat && OPPORTUNITY_TYPE_FILTER_LABEL[cat]) {
    return OPPORTUNITY_TYPE_FILTER_LABEL[cat];
  }
  const raw =
    text(opp.segmentLabel) ||
    text(opp.demandSegment) ||
    text(opp.segmentId) ||
    text(opp.researchSegment);
  if (raw && !/^unspecified$/i.test(raw)) {
    if (OPPORTUNITY_TYPE_FILTER_LABEL[raw]) {
      return OPPORTUNITY_TYPE_FILTER_LABEL[raw];
    }
    return raw;
  }
  return "Unclassified";
}

export function resolveCanonicalOpportunityTitle(opp = {}) {
  return (
    text(opp.displayTitle) ||
    text(opp.title) ||
    text(opp.organizationName) ||
    oppId(opp) ||
    "Opportunity"
  );
}

export function resolveDemandTimingDisplay(opp = {}) {
  const established =
    text(opp.eventDateDisplay) ||
    text(opp.datePillDisplayLabel) ||
    text(opp.eventDates) ||
    text(opp.demandPeriodLabel) ||
    text(opp.timingSummary);
  if (established) {
    return { label: established, kind: "ESTABLISHED_EVENT_DATE" };
  }
  const start = text(opp.eventStartDate);
  const end = text(opp.eventEndDate);
  if (start) {
    return {
      label: end && end !== start ? `${start} – ${end}` : start,
      kind: "ESTABLISHED_EVENT_DATE",
    };
  }
  const estimated = text(opp.expectedDemandPeriod) || text(opp.estimatedDemandWindow);
  if (estimated) return { label: estimated, kind: "ESTIMATED_DEMAND_PERIOD" };
  if (
    /SCALING|RELOCATION|PROJECTS|LOCAL_CATERING|CORPORATE_ACCOUNT/i.test(
      text(opp.opportunityTypeFilterCategory) + text(opp.segmentId),
    )
  ) {
    return { label: "Ongoing account relationship", kind: "ONGOING_ACCOUNT" };
  }
  return { label: "Timing not yet established", kind: "TIMING_NOT_ESTABLISHED" };
}

function contactActionabilityLabel(opp) {
  let tier =
    opp?.contactActionabilityTier ||
    classifyContactActionability(opp)?.tier ||
    CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED;
  // Prefer honest reachable state when a named person + professional channel exists.
  if (
    hasNamedContact(opp) &&
    hasOutreachChannel(opp) &&
    (tier === CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED ||
      tier === CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED ||
      !tier)
  ) {
    tier = CONTACT_ACTIONABILITY_TIER.REACHABLE;
  } else if (
    hasNamedContact(opp) &&
    !hasOutreachChannel(opp) &&
    tier === CONTACT_ACTIONABILITY_TIER.CONTACT_IDENTIFICATION_REQUIRED
  ) {
    tier = CONTACT_ACTIONABILITY_TIER.CONTACT_RESEARCH_REQUIRED;
  }
  return {
    tier,
    label: CONTACT_TIER_LABEL[tier] || "Contact Status Unavailable",
  };
}

function hasNamedContact(opp) {
  return Boolean(
    text(opp?.primaryContact?.name) ||
      text(opp?.primaryContact?.fullName) ||
      text(opp?.tileContactPreview?.name) ||
      text(opp?.contactName),
  );
}

function hasOutreachChannel(opp) {
  const c = opp?.primaryContact || {};
  return Boolean(
    text(c.email) ||
      text(c.verifiedEmail) ||
      text(c.phone) ||
      text(c.linkedinUrl) ||
      text(c.sourceUrl),
  );
}

function collectEvidenceUrls(opp) {
  const urls = new Set();
  for (const u of opp?.evidenceUrls || []) {
    if (/^https?:\/\//i.test(String(u))) urls.add(String(u).split("?")[0]);
  }
  for (const e of opp?.evidence || []) {
    const u = e?.sourceUrl || e?.url;
    if (/^https?:\/\//i.test(String(u))) urls.add(String(u).split("?")[0]);
  }
  for (const u of opp?.seedUrls || []) {
    if (/^https?:\/\//i.test(String(u))) urls.add(String(u).split("?")[0]);
  }
  const contactUrl = opp?.primaryContact?.sourceUrl || opp?.primaryContact?.linkedinUrl;
  if (/^https?:\/\//i.test(String(contactUrl))) {
    urls.add(String(contactUrl).split("?")[0]);
  }
  return urls;
}

function hasDeepResearchMarkers(opp) {
  return Boolean(
    opp?.deepResearchCompleted === true ||
      opp?.dossierDepth === "DEEP" ||
      opp?.researchDepth === "DEEP" ||
      (Array.isArray(opp?.evidence) && opp.evidence.length >= 3) ||
      text(opp?.hotelOpportunityThesis) ||
      text(opp?.whyThisHotel) ||
      text(opp?.whatIsHappening) ||
      text(opp?.potentialHotelBusiness),
  );
}

function competitiveCount(opportunities = []) {
  return opportunities.filter(
    (o) =>
      o?.competitiveMarketScopeEligible === true &&
      o?.hotelMarketScopeEligible !== true,
  ).length;
}

function hotelMarketCount(opportunities = []) {
  return opportunities.filter((o) => o?.hotelMarketScopeEligible !== false).length;
}

/**
 * Demand composition for charts — canonical Opportunity Type taxonomy.
 */
export function buildDemandComposition(opportunities = []) {
  const counts = {};
  for (const opp of opportunities) {
    const label = resolveCanonicalDemandSegmentLabel(opp);
    counts[label] = (counts[label] || 0) + 1;
  }
  const rows = Object.entries(counts)
    .map(([name, count]) => ({ name, count }))
    .sort((a, b) => b.count - a.count || a.name.localeCompare(b.name));
  return {
    grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
    total: opportunities.length,
    rows,
    chartKind: "HORIZONTAL_BAR",
    unspecifiedCount: rows
      .filter((r) => /unspecified|unclassified/i.test(r.name))
      .reduce((n, r) => n + r.count, 0),
  };
}

/**
 * Commercial demand outlook by month — only evidenced dates.
 */
export function buildCommercialDemandOutlook(opportunities = []) {
  const byMonth = new Map();
  const buckets = {
    establishedEventDates: 0,
    estimatedDemandPeriods: 0,
    ongoingAccountRelationships: 0,
    timingNotEstablished: 0,
  };
  for (const opp of opportunities) {
    const timing = resolveDemandTimingDisplay(opp);
    if (timing.kind === "ESTABLISHED_EVENT_DATE") buckets.establishedEventDates += 1;
    else if (timing.kind === "ESTIMATED_DEMAND_PERIOD") buckets.estimatedDemandPeriods += 1;
    else if (timing.kind === "ONGOING_ACCOUNT") buckets.ongoingAccountRelationships += 1;
    else buckets.timingNotEstablished += 1;

    const start = text(opp.eventStartDate);
    if (!/^\d{4}-\d{2}/.test(start)) continue;
    const key = start.slice(0, 7);
    const row = byMonth.get(key) || { month: key, count: 0, opportunityIds: [] };
    row.count += 1;
    row.opportunityIds.push(oppId(opp));
    byMonth.set(key, row);
  }
  const timeline = [...byMonth.values()].sort((a, b) =>
    a.month.localeCompare(b.month),
  );
  return {
    grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
    timeline,
    buckets,
    chartKind: "TIMELINE_BAR",
    note:
      timeline.length === 0
        ? "No established event months in this inventory — dates are not invented."
        : null,
  };
}

export function prioritizeCustomerOpportunitiesV2(opportunities = [], limit = 4) {
  const base = prioritizeV1(opportunities, limit);
  return base.map((item) => {
    const opp =
      opportunities.find((o) => oppId(o) === item.opportunityId) || {};
    const timing = resolveDemandTimingDisplay(opp);
    const contact = contactActionabilityLabel(opp);
    return {
      ...item,
      name: resolveCanonicalOpportunityTitle(opp) || item.name,
      segment: resolveCanonicalDemandSegmentLabel(opp),
      demandTiming: timing.label,
      demandTimingKind: timing.kind,
      recommendedNextAction:
        text(opp.salesActionPillLabel) ||
        text(opp.recommendedSalesAction) ||
        item.recommendedNextAction ||
        null,
      contactActionabilityTier: contact.tier,
      contactActionabilityLabel: contact.label,
      whyItMatters:
        text(opp.whyActNow) ||
        text(opp.cardWhyNowLine) ||
        text(item.hotelRationale)?.slice(0, 220) ||
        null,
      hotelRationale: text(item.hotelRationale)?.slice(0, 280) || null,
    };
  });
}

function buildWeeklyChangeIntegrity(opportunities = []) {
  const hasAnyDelta = opportunities.some((o) =>
    Boolean(
      text(o.weeklyDeltaState) ||
        text(o.weeklyChangeState) ||
        text(o.deltaState),
    ),
  );
  const counts = weeklyDeltaSummaryCounts(opportunities);
  if (!hasAnyDelta) {
    return {
      engine: "weekly-delta.js",
      secondEngineProhibited: true,
      comparisonAvailable: false,
      comparisonKind: "UNAVAILABLE_COMPARISON_HISTORY",
      counts: null,
      note:
        "No Weekly Change markers on this inventory — missing history is not reported as zero new or zero changed.",
    };
  }
  return {
    engine: "weekly-delta.js",
    secondEngineProhibited: true,
    comparisonAvailable: true,
    comparisonKind: "WEEKLY_DELTA_MARKERS_PRESENT",
    counts,
    note: null,
  };
}

function extractDateFromPublicationVersion(version) {
  const m = String(version || "").match(/_(\d{8})(?:_|$)/);
  if (!m) return null;
  const raw = m[1];
  const y = raw.slice(0, 4);
  const mo = raw.slice(4, 6);
  const d = raw.slice(6, 8);
  if (!y || !mo || !d) return null;
  return `${y}-${mo}-${d}`;
}

function buildReportingPeriod(input = {}) {
  const founderPreview = input.founderPreview === true;
  const publishedAt =
    input.lastResearchAt ||
    input.publicationMeta?.publishedAt ||
    input.publicationMeta?.generatedAt ||
    input.publicationMeta?.createdAt ||
    null;
  const version =
    input.publicationMeta?.publicationVersion ||
    input.publicationMeta?.previewSource ||
    null;
  const versionDate = extractDateFromPublicationVersion(version);
  const asOf = publishedAt || (versionDate ? `${versionDate}T00:00:00.000Z` : null);
  const asOfDay = asOf ? String(asOf).slice(0, 10) : null;
  let label;
  if (asOfDay) {
    label = founderPreview
      ? `STAGED research as of ${asOfDay}`
      : `CURRENT research as of ${asOfDay}`;
  } else if (founderPreview) {
    label = "Latest STAGED research package";
  } else {
    label = "Latest CURRENT publication";
  }
  return {
    label,
    kind: founderPreview ? "STAGED_PACKAGE" : "CURRENT_PUBLICATION",
    visibilityContext: founderPreview ? "FOUNDER_PREVIEW" : "CUSTOMER_CURRENT",
    visibilityContextNote:
      "Founder Preview is a visibility context, not a reporting window.",
    asOf,
    publicationVersion: version,
  };
}

function buildCustomerResearchReceipt(opportunities = [], opts = {}) {
  const evidence = new Set();
  let deep = 0;
  let namedContacts = 0;
  let outreach = 0;
  for (const opp of opportunities) {
    for (const u of collectEvidenceUrls(opp)) evidence.add(u);
    if (hasDeepResearchMarkers(opp)) deep += 1;
    if (hasNamedContact(opp)) namedContacts += 1;
    if (hasOutreachChannel(opp)) outreach += 1;
  }
  const weekly = buildWeeklyChangeIntegrity(opportunities);
  const metrics = [
    {
      key: "qualifiedOpportunitiesDelivered",
      label: "Qualified Opportunities Delivered",
      value: opportunities.length,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING,
    },
    {
      key: "opportunitiesDeeplyResearched",
      label: "Opportunities Deeply Researched",
      value: deep,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    },
    {
      key: "evidenceSourcesReviewed",
      label: "Evidence Sources Reviewed",
      value: evidence.size,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability:
        evidence.size > 0
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
      reason:
        evidence.size > 0
          ? null
          : "Evidence URLs not present on all package rows",
    },
    {
      key: "commercialContactsIdentified",
      label: "Commercial Contacts Identified",
      value: namedContacts,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    },
    {
      key: "professionalOutreachChannels",
      label: "Professional Outreach Channels",
      value: outreach,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    },
    {
      key: "newIntelligenceThisPeriod",
      label: "New Intelligence This Period",
      value: weekly.comparisonAvailable ? Number(weekly.counts?.newThisWeek) || 0 : null,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability: weekly.comparisonAvailable
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      reason: weekly.comparisonAvailable ? null : weekly.note,
    },
    {
      key: "existingIntelligenceUpdated",
      label: "Existing Intelligence Updated",
      value: weekly.comparisonAvailable
        ? (Number(weekly.counts?.updated) || 0) +
          (Number(weekly.counts?.reactivated) || 0)
        : null,
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      availability: weekly.comparisonAvailable
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      reason: weekly.comparisonAvailable ? null : weekly.note,
    },
  ].map((m) => ({
    ...buildResearchReceiptMetric(m.key, {
      value: m.value,
      availability: m.availability,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
      reason: m.reason || null,
    }),
    label: m.label,
    grain: m.grain,
    customerFacing: true,
  }));

  const productivity = opts.researchProductivity || null;

  return {
    contractId: GDI_RESEARCH_RECEIPT_CONTRACT_ID,
    audience: "CUSTOMER_FACING",
    hotelId: opts.hotelId || null,
    reportingPeriod: opts.reportingPeriod || null,
    metrics,
    researchProductivity: productivity
      ? {
          headline: productivity.customerHeadline,
          lede: productivity.customerLede,
          methodologySummary: productivity.methodologySummary,
          disclaimer: productivity.disclaimer,
          estimateClass: productivity.estimateClass,
          benchmarkVersion: productivity.benchmarkVersion,
          metrics: [
            productivity.customerMetrics.estimatedResearchHoursSaved,
            productivity.customerMetrics.equivalentWorkingDaysSaved,
            productivity.customerMetrics.estimatedResearchLaborValue,
          ],
          equivalentManualResearchHours:
            productivity.customerMetrics.equivalentManualResearchHours,
        }
      : null,
    provenance: {
      grainNote:
        "Customer receipt uses qualified inventory grain — not raw 161 discovery volume.",
      syntheticCountsProhibited: true,
    },
  };
}

function buildInternalResearchIntelligence(opportunities = [], opts = {}) {
  const evidence = new Set();
  let deep = 0;
  let named = 0;
  let withEmailOrPhone = 0;
  for (const opp of opportunities) {
    for (const u of collectEvidenceUrls(opp)) evidence.add(u);
    if (hasDeepResearchMarkers(opp)) deep += 1;
    if (hasNamedContact(opp)) named += 1;
    if (hasOutreachChannel(opp)) withEmailOrPhone += 1;
  }
  const composition = buildDemandComposition(opportunities);
  const productivity = opts.researchProductivity || null;
  const internal = productivity?.internalMetrics || {};
  return {
    audience: "FOUNDER_ADMIN_ONLY",
    grain: METRIC_GRAIN.RESEARCH_ACTIVITY,
    restricted: true,
    metrics: [
      {
        key: "qualifiedInventoryCount",
        label: "Qualified Inventory (STAGED/CURRENT)",
        value: opportunities.length,
        availability: "AVAILABLE",
        note: "Not equal to raw discovery volume (e.g. 161).",
      },
      {
        key: "deeplyResearchedInInventory",
        label: "Deeply Researched In Qualified Inventory",
        value: deep,
        availability: "DERIVED",
      },
      {
        key: "evidenceUrlsInInventory",
        label: "Evidence URLs On Qualified Rows",
        value: evidence.size,
        availability: "DERIVED",
      },
      {
        key: "namedContactsInInventory",
        label: "Named Contacts On Qualified Rows",
        value: named,
        availability: "DERIVED",
      },
      {
        key: "outreachChannelsInInventory",
        label: "Outreach Channels On Qualified Rows",
        value: withEmailOrPhone,
        availability: "DERIVED",
      },
      {
        key: "demandSegmentCoverage",
        label: "Demand Segments Represented",
        value: composition.rows.length,
        availability: "DERIVED",
      },
      {
        key: "rawDiscoveryVolume",
        label: "Raw Discovery Volume",
        value: null,
        availability: "UNAVAILABLE",
        note:
          "Discovery grain is separate from qualified inventory; do not conflate with STAGED count.",
      },
      {
        key: "equivalentManualResearchHours",
        label: "Equivalent Manual Research Hours",
        value: internal.equivalentManualResearchHours?.value ?? null,
        availability:
          internal.equivalentManualResearchHours?.availability ===
          RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
            ? "DERIVED"
            : "UNAVAILABLE",
        note: "Provisional benchmark × verified activity volumes.",
      },
      {
        key: "actualHumanResearchHours",
        label: "Actual Human Research And Review Effort",
        value: internal.actualHumanResearchAndReviewHours?.value ?? null,
        availability:
          internal.actualHumanResearchAndReviewHours?.value != null
            ? "AVAILABLE"
            : "UNAVAILABLE",
        note: "Measured Dealality human effort only — not AI wall-clock time.",
      },
      {
        key: "researchProductivityMultiple",
        label: "Research Productivity Multiple",
        value: internal.researchProductivityMultiple?.value ?? null,
        availability:
          internal.researchProductivityMultiple?.value != null
            ? "DERIVED"
            : "UNAVAILABLE",
      },
      {
        key: "researchSpendUsd",
        label: "Research Spend",
        value: internal.actualProviderExpenditureUsd?.value ?? null,
        availability:
          internal.actualProviderExpenditureUsd?.value != null
            ? "AVAILABLE"
            : "UNAVAILABLE",
        note: "Ledger join not loaded for this dashboard view.",
      },
      {
        key: "costPerQualifiedOpportunity",
        label: "Cost Per Qualified Opportunity",
        value: internal.costPerQualifiedOpportunity?.value ?? null,
        availability:
          internal.costPerQualifiedOpportunity?.value != null
            ? "DERIVED"
            : "UNAVAILABLE",
      },
      {
        key: "benchmarkValidationStatus",
        label: "Benchmark Validation Status",
        value: internal.benchmarkValidationStatus || "PROVISIONAL",
        availability: "AVAILABLE",
      },
      {
        key: "contentHydrationQuality",
        label: "Content Hydration Quality",
        value: composition.unspecifiedCount === 0 ? "SEGMENTS_RESOLVED" : "PARTIAL",
        availability: "DERIVED",
      },
    ],
    researchProductivity: productivity
      ? {
          estimateClass: productivity.estimateClass,
          benchmarkVersion: productivity.benchmarkVersion,
          activityLines: productivity.activityLines,
          internalMetrics: productivity.internalMetrics,
        }
      : null,
    demandComposition: composition,
    publication: {
      dataState: opts.dataState || null,
      publicationVersion: opts.reportingPeriod?.publicationVersion || null,
      founderPreview: opts.founderPreview === true,
    },
  };
}

function buildExecutiveAssessment(opportunities = [], composition, outlook) {
  const topSeg = (composition.rows || []).slice(0, 3).map((r) => r.name);
  const competitive = competitiveCount(opportunities);
  const weekly = buildWeeklyChangeIntegrity(opportunities);
  return {
    marketOpportunity:
      opportunities.length > 0
        ? `${opportunities.length} qualified opportunities are available for commercial review.`
        : "No qualified opportunities are available in this publication state.",
    strongestCommercialDemandSegments: topSeg.length
      ? topSeg.join(", ")
      : "Demand segments not yet classified.",
    immediateCommercialFocus:
      opportunities.filter((o) =>
        /BUILD_ACCOUNT|QUALIFY|PURSUE|CONTACT/i.test(
          text(o.salesActionKey) + text(o.salesActionPillLabel),
        ),
      ).length > 0
        ? "Prioritize Build Account and Qualify Opportunity actions with named contacts."
        : "Review priority opportunities and confirm sales actions with the hotel team.",
    competitiveCapturePotential:
      competitive > 0
        ? `${competitive} competitive-scope opportunities are present.`
        : "No competitive-only opportunities in this qualified inventory.",
    materialChangesSinceLastResearch: weekly.comparisonAvailable
      ? `New ${weekly.counts?.newThisWeek || 0}; updated ${weekly.counts?.updated || 0}; reactivated ${weekly.counts?.reactivated || 0}.`
      : "Comparable Weekly Change history is not available for this package.",
    businessImpact:
      outlook.timeline.length > 0
        ? `Evidenced booking windows span ${outlook.timeline[0].month} to ${outlook.timeline[outlook.timeline.length - 1].month}.`
        : "Most opportunities are ongoing accounts or do not yet have established event months.",
  };
}

function buildSalesPerformance(opportunities = [], engagement, productivity = null) {
  const ready = opportunities.filter((o) => {
    const t = contactActionabilityLabel(o).tier;
    return (
      t === CONTACT_ACTIONABILITY_TIER.SALES_READY ||
      t === CONTACT_ACTIONABILITY_TIER.REACHABLE ||
      hasOutreachChannel(o)
    );
  }).length;
  const upcoming = opportunities.filter((o) => {
    const start = text(o.eventStartDate);
    if (!start) return false;
    const t = Date.parse(start);
    return Number.isFinite(t) && t >= Date.now();
  }).length;
  const buildAccount = opportunities.filter((o) =>
    /build account/i.test(text(o.salesActionPillLabel)),
  ).length;
  const outlook = buildCommercialDemandOutlook(opportunities);

  return {
    kpis: [
      { key: "readyToPursue", label: "Ready To Pursue", value: ready },
      {
        key: "newOpportunities",
        label: "New Opportunities",
        value: null,
        availability: "UNAVAILABLE",
        note: "Weekly Change history unavailable — not reported as zero.",
      },
      { key: "upcomingDemand", label: "Upcoming Demand", value: upcoming },
      {
        key: "reachableContacts",
        label: "Reachable Contacts",
        value: opportunities.filter((o) => hasOutreachChannel(o)).length,
      },
      {
        key: "activePursuits",
        label: "Active Pursuits",
        value: engagement?.opportunitiesPursued?.value ?? null,
        availability: engagement?.opportunitiesPursued?.availability || "UNAVAILABLE",
      },
      {
        key: "businessWon",
        label: "Business Won",
        value: engagement?.opportunitiesWon?.value ?? null,
        availability: engagement?.opportunitiesWon?.availability || "UNAVAILABLE",
      },
    ],
    funnel: {
      unit: "qualified_opportunity_or_pursuit_record",
      reportingPeriod: "hotel_lifetime_loaded_records",
      stages: [
        { key: "qualified", label: "Qualified", value: opportunities.length },
        {
          key: "pursued",
          label: "Pursued",
          value: engagement?.opportunitiesPursued?.value ?? null,
        },
        {
          key: "rfp",
          label: "RFP / Proposal",
          value: engagement?.rfpOrProposalActivity?.value ?? null,
        },
        {
          key: "won",
          label: "Won",
          value: engagement?.opportunitiesWon?.value ?? null,
        },
        {
          key: "lost",
          label: "Lost",
          value: engagement?.opportunitiesLost?.value ?? null,
        },
      ],
      note: "Funnel stages use verified pursuit/feedback records only — no fabricated progression.",
    },
    commercialFocus: {
      immediatePursuit: opportunities
        .filter((o) => /qualify|pursue|build account/i.test(text(o.salesActionPillLabel)))
        .slice(0, 5)
        .map((o) => ({
          opportunityId: oppId(o),
          title: resolveCanonicalOpportunityTitle(o),
          salesAction: text(o.salesActionPillLabel) || null,
        })),
      upcomingBookingWindows: outlook.timeline.slice(0, 6),
      corporateAccountDevelopment: buildAccount,
      competitiveCapture: competitiveCount(opportunities),
      needPeriodOpportunities: opportunities.filter(
        (o) => o?.needDateMatch === true,
      ).length,
    },
    researchTimeSaved: productivity
      ? {
          headline: "Where Research Time Was Saved",
          disclaimer: productivity.disclaimer,
          breakdown: productivity.salesTimeSavedBreakdown,
          estimatedResearchHoursSaved:
            productivity.customerMetrics.estimatedResearchHoursSaved,
        }
      : null,
  };
}

/**
 * Build Executive Summary V2 payload with audience perspectives.
 *
 * @param {object} input — same as V1 plus:
 * @param {boolean} [input.includeResearchIntelligence]
 * @param {string} [input.perspective]
 */
export function buildGdiExecutiveSummaryV2(input = {}) {
  const includeResearchIntelligence = input.includeResearchIntelligence === true;
  const base = buildGdiExecutiveSummaryV1(input);
  const founderPreview = input.founderPreview === true;
  const customerAuthorized = input.customerAuthorizedInventory === true;
  const opportunities =
    customerAuthorized || founderPreview
      ? Array.isArray(input.opportunities)
        ? input.opportunities
        : []
      : [];

  const reportingPeriod = buildReportingPeriod(input);
  const composition = buildDemandComposition(opportunities);
  const outlook = buildCommercialDemandOutlook(opportunities);
  const weekly = buildWeeklyChangeIntegrity(opportunities);
  const engagement = buildCommercialEngagementSection({
    pursuits: input.pursuits,
    feedback: input.feedback,
  });
  const priorities = prioritizeCustomerOpportunitiesV2(opportunities, 4);
  const priorityOpportunityRows = priorities
    .map((p) =>
      opportunities.find((o) => oppId(o) === text(p.opportunityId)),
    )
    .filter(Boolean);
  const researchProductivity = buildGdiResearchProductivity({
    opportunities,
    weeklyComparisonAvailable: weekly.comparisonAvailable === true,
    weeklyChangedCount: weekly.comparisonAvailable
      ? (Number(weekly.counts?.updated) || 0) +
        (Number(weekly.counts?.reactivated) || 0)
      : null,
    screenedOutCount:
      input.screenedOutCount == null ? null : Number(input.screenedOutCount),
    laborCostAuthorized: input.laborCostAuthorized === true,
    fullyLoadedLaborCostPerHour: input.fullyLoadedLaborCostPerHour,
    dealalityInvestmentAuthorized: input.dealalityInvestmentAuthorized === true,
    dealalityInvestmentForPeriod: input.dealalityInvestmentForPeriod,
    actualHumanResearchHours: input.actualHumanResearchHours,
    providerSpendUsd: input.providerSpendUsd,
    estimateCase: input.productivityEstimateCase || "base",
  });
  const receipt = buildCustomerResearchReceipt(opportunities, {
    hotelId: input.hotelId,
    reportingPeriod,
    researchProductivity,
  });
  const assessment = buildExecutiveAssessment(
    opportunities,
    composition,
    outlook,
  );

  const actionable = opportunities.filter((o) => {
    const t = contactActionabilityLabel(o).tier;
    return (
      t === CONTACT_ACTIONABILITY_TIER.SALES_READY ||
      t === CONTACT_ACTIONABILITY_TIER.REACHABLE ||
      hasOutreachChannel(o)
    );
  }).length;

  const snapshot = {
    totalQualifiedOpportunities: opportunities.length,
    newOpportunitiesSincePriorPeriod: weekly.comparisonAvailable
      ? Number(weekly.counts?.newThisWeek) || 0
      : null,
    newOpportunitiesAvailability: weekly.comparisonAvailable
      ? "AVAILABLE"
      : "UNAVAILABLE",
    materiallyChangedOpportunities: weekly.comparisonAvailable
      ? (Number(weekly.counts?.updated) || 0) +
        (Number(weekly.counts?.reactivated) || 0)
      : null,
    competitiveOpportunities: competitiveCount(opportunities),
    hotelMarketOpportunities: hotelMarketCount(opportunities),
    actionableOpportunities: actionable,
    opportunitiesAlignedWithNeedPeriods: opportunities.filter(
      (o) => o?.needDateMatch === true,
    ).length,
    estimatedRoomNights: null,
    estimatedRoomNightsNote:
      "Estimated room nights are hidden until evidenced on opportunity rows.",
    priorityDemandSegments: composition.rows.slice(0, 5),
    reportingPeriod,
    lastResearchCompletionDate: reportingPeriod.asOf,
    distinguishesOpportunitiesFromContacts: true,
  };

  const executiveOverview = {
    perspective: EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW,
    commercialKpiRibbon: [
      { key: "qualified", label: "Qualified Opportunities", value: snapshot.totalQualifiedOpportunities },
      {
        key: "new",
        label: "New This Period",
        value: snapshot.newOpportunitiesSincePriorPeriod,
        availability: snapshot.newOpportunitiesAvailability,
      },
      { key: "competitive", label: "Competitive Opportunities", value: snapshot.competitiveOpportunities },
      { key: "actionable", label: "Actionable Opportunities", value: snapshot.actionableOpportunities },
      {
        key: "needPeriods",
        label: "Need Periods Supported",
        value: snapshot.opportunitiesAlignedWithNeedPeriods,
      },
    ],
    executiveAssessment: assessment,
    demandComposition: composition,
    commercialDemandOutlook: outlook,
    customerResearchReceipt: receipt,
    priorityCommercialOpportunities: {
      max: 4,
      count: priorities.length,
      opportunityOfTheWeek: priorities[0] || null,
      items: priorities,
      /** Full canonical opportunity rows for shared tile component (not slim cards). */
      opportunityRows: priorityOpportunityRows,
      sharedTileComponent: "DealalityGdiUi.opportunityTileHtml",
      viewAllOpportunitiesTab: "opportunities",
    },
    commercialOutcomes: engagement,
    researchProductivity: {
      headline: researchProductivity.customerHeadline,
      lede: researchProductivity.customerLede,
      methodologySummary: researchProductivity.methodologySummary,
      disclaimer: researchProductivity.disclaimer,
      estimateClass: researchProductivity.estimateClass,
      benchmarkVersion: researchProductivity.benchmarkVersion,
      metrics: [
        researchProductivity.customerMetrics.estimatedResearchHoursSaved,
        researchProductivity.customerMetrics.equivalentWorkingDaysSaved,
        researchProductivity.customerMetrics.estimatedResearchLaborValue,
      ],
      equivalentManualResearchHours:
        researchProductivity.customerMetrics.equivalentManualResearchHours,
    },
    investmentRecovery: {
      ...buildInvestmentRecoveryPlaceholder(),
      categories: researchProductivity.investmentRecoverySplit,
      customerCopy:
        "Category A is estimated research productivity value. Category B is verified incremental commercial value. These are never combined into a single realized ROI claim. Redeployable time is not payroll savings.",
      hideRawFieldKeys: true,
    },
  };

  const salesPerformance = buildSalesPerformance(
    opportunities,
    engagement,
    researchProductivity,
  );

  const payload = {
    ...base,
    version: GDI_EXECUTIVE_SUMMARY_V2,
    authorizationId: GDI_EXECUTIVE_SUMMARY_V2_AUTH,
    v1AuthorizationId: GDI_EXECUTIVE_SUMMARY_AUTH,
    uiVersion: "v2",
    perspectives: {
      default: EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW,
      available: [
        EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW,
        EXEC_SUMMARY_PERSPECTIVE.SALES_PERFORMANCE,
        ...(includeResearchIntelligence
          ? [EXEC_SUMMARY_PERSPECTIVE.RESEARCH_INTELLIGENCE]
          : []),
      ],
    },
    sections: {
      ...base.sections,
      executiveCommercialSnapshot: {
        ...snapshot,
        ...(founderPreview
          ? {
              stagedStatusIndicator: "STAGED",
              founderPreviewBanner: null,
            }
          : {}),
      },
      researchReceipt: receipt,
      researchProductivity: includeResearchIntelligence
        ? researchProductivity
        : {
            contractId: researchProductivity.contractId,
            benchmarkVersion: researchProductivity.benchmarkVersion,
            estimateClass: researchProductivity.estimateClass,
            disclaimer: researchProductivity.disclaimer,
            customerHeadline: researchProductivity.customerHeadline,
            customerLede: researchProductivity.customerLede,
            methodologySummary: researchProductivity.methodologySummary,
            customerMetrics: researchProductivity.customerMetrics,
            salesTimeSavedBreakdown:
              researchProductivity.salesTimeSavedBreakdown,
            investmentRecoverySplit:
              researchProductivity.investmentRecoverySplit,
            governance: {
              provisionalBenchmarks: true,
              providerCostsCustomerFacingProhibited: true,
              estimatedLaborNotRealizedRoi: true,
            },
          },
      priorityCommercialOpportunities: {
        max: 4,
        count: priorities.length,
        opportunityOfTheWeek: priorities[0] || null,
        items: priorities,
        opportunityRows: priorityOpportunityRows,
        sharedTileComponent: "DealalityGdiUi.opportunityTileHtml",
        rankingNote:
          "Reuses published scores, contact actionability, need-period alignment, and weekly-delta — not a new ranking engine.",
      },
      demandComposition: composition,
      commercialDemandOutlook: outlook,
      executiveOverview,
      salesPerformance,
      ...(includeResearchIntelligence
        ? {
            researchIntelligence: buildInternalResearchIntelligence(
              opportunities,
              {
                dataState: input.dataState,
                reportingPeriod,
                founderPreview,
                researchProductivity,
              },
            ),
          }
        : {}),
    },
    weeklyChange: weekly,
    metricIntegrity: {
      grains: Object.values(METRIC_GRAIN),
      qualifiedInventoryNotRawDiscovery: true,
      whoHowOnQualifiedInventoryOnly: true,
      founderPreviewIsVisibilityNotPeriod: true,
      researchProductivityProvisional: true,
      estimatedLaborNotRealizedRoi: true,
    },
    pdfPreparation: {
      ...base.pdfPreparation,
      designedFor: "EXECUTIVE_OVERVIEW",
      implemented: false,
      downloadControl: "HIDDEN_UNTIL_CERTIFIED",
    },
  };

  return payload;
}

/**
 * Strip internal admin dashboards from a customer-facing ES payload
 * (defense in depth). Sales Performance + Research Intelligence are
 * Admin Resources only — never emit via ordinary customer API.
 */
export function stripResearchIntelligencePerspective(payload = {}) {
  if (!payload || typeof payload !== "object") return payload;
  const next = { ...payload, sections: { ...(payload.sections || {}) } };
  delete next.sections.researchIntelligence;
  delete next.sections.salesPerformance;
  if (next.sections.researchProductivity?.internalMetrics) {
    const rp = { ...next.sections.researchProductivity };
    delete rp.internalMetrics;
    delete rp.activityLines;
    next.sections.researchProductivity = rp;
  }
  // Customer ES has no secondary perspective tabs.
  next.perspectives = {
    default: EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW,
    available: [EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW],
    secondaryTabsRemoved: true,
    adminDashboards:
      "GDI Sales Performance + GDI Research Intelligence under Admin Resources",
  };
  next.customerFacingLayout = "consolidated-executive-summary";
  return next;
}

/** Alias — strips both internal admin dashboard sections. */
export function stripInternalAdminDashboards(payload = {}) {
  return stripResearchIntelligencePerspective(payload);
}

export default {
  GDI_EXECUTIVE_SUMMARY_V2,
  buildGdiExecutiveSummaryV2,
  stripResearchIntelligencePerspective,
  stripInternalAdminDashboards,
  buildDemandComposition,
  buildCommercialDemandOutlook,
  prioritizeCustomerOpportunitiesV2,
  resolveCanonicalDemandSegmentLabel,
  resolveCanonicalOpportunityTitle,
};
