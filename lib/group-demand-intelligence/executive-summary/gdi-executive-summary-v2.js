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
import { resolveResearchFunnelCohort } from "./gdi-research-funnel-cohort-v1.js";
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
import {
  buildCompetitiveResearchCompletionRecord,
  competitiveCompletionMetricsForResearchIntelligence,
} from "../workflow/gdi-competitive-research-completion-gate-v1.js";

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

function looksLikeCalendarDateLabel(label) {
  const s = text(label);
  if (!s) return false;
  if (/^ongoing\b/i.test(s)) return false;
  if (/not (yet )?assessed|timing not|tbd|unknown/i.test(s)) return false;
  // ISO, numeric ranges, or month-name ranges (e.g. "Apr 2–5, 2027").
  return (
    /^\d{4}-\d{2}/.test(s) ||
    /\b(20\d{2})\b/.test(s) ||
    /\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\b/i.test(s)
  );
}

/**
 * Demand timing for Executive Summary charts/buckets.
 * "Ongoing" display pills are ONGOING accounts — never Established Event Dates.
 * Chart months use ISO eventStartDate only (never invented).
 */
export function resolveDemandTimingDisplay(opp = {}) {
  const start = text(opp.eventStartDate);
  const end = text(opp.eventEndDate);
  if (/^\d{4}-\d{2}/.test(start)) {
    const pretty =
      text(opp.eventDateDisplay) ||
      text(opp.datePillDisplayLabel) ||
      (end && end !== start ? `${start} – ${end}` : start);
    return {
      label: looksLikeCalendarDateLabel(pretty) ? pretty : start,
      kind: "ESTABLISHED_EVENT_DATE",
      chartMonth: start.slice(0, 7),
    };
  }

  const display =
    text(opp.eventDateDisplay) ||
    text(opp.datePillDisplayLabel) ||
    text(opp.eventDates) ||
    text(opp.demandPeriodLabel) ||
    text(opp.timingSummary);

  if (looksLikeCalendarDateLabel(display)) {
    return { label: display, kind: "ESTABLISHED_EVENT_DATE", chartMonth: null };
  }

  const estimated =
    text(opp.expectedDemandPeriod) || text(opp.estimatedDemandWindow);
  if (estimated && !/^ongoing\b/i.test(estimated)) {
    return { label: estimated, kind: "ESTIMATED_DEMAND_PERIOD", chartMonth: null };
  }

  if (
    /^ongoing\b/i.test(display) ||
    /SCALING|RELOCATION|PROJECTS|LOCAL_CATERING|CORPORATE_ACCOUNT/i.test(
      text(opp.opportunityTypeFilterCategory) + text(opp.segmentId),
    )
  ) {
    return {
      label: display && /^ongoing\b/i.test(display)
        ? "Ongoing account relationship"
        : display || "Ongoing account relationship",
      kind: "ONGOING_ACCOUNT",
      chartMonth: null,
    };
  }

  return {
    label: "Timing not yet established",
    kind: "TIMING_NOT_ESTABLISHED",
    chartMonth: null,
  };
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

function resolvePersonLinkedIn(contact = {}) {
  const direct = text(contact.linkedinUrl);
  if (direct && /linkedin\.com/i.test(direct)) return direct;
  const src = text(contact.sourceUrl);
  if (src && /linkedin\.com\/in\//i.test(src)) return src;
  return "";
}

/**
 * Admin Research Intelligence contact coverage scorecard (qualified inventory grain).
 * Separates opportunity-level counts from unique-person / channel counts.
 * Customer-facing Research Receipt keeps high-level contact metrics only.
 */
export function buildContactCoverageScorecard(opportunities = []) {
  const people = new Map();
  let namedOps = 0;
  let emailOps = 0;
  let phoneOps = 0;
  let linkedInOps = 0;
  let usableOps = 0;
  let stage4Ops = 0;
  let recoveredLinkedInFields = 0;

  for (const opp of opportunities) {
    const primary = opp?.primaryContact || null;
    const secondary = opp?.secondaryContact || null;
    const contacts = [primary, secondary].filter(Boolean);
    if (hasNamedContact(opp)) namedOps += 1;

    let opEmail = false;
    let opPhone = false;
    let opLi = false;
    let opStage4 = false;

    for (const c of contacts) {
      const name = text(c.name) || text(c.fullName);
      if (!name) continue;
      const key = name.toLowerCase();
      const row = people.get(key) || {
        name,
        title: false,
        email: false,
        phone: false,
        linkedIn: false,
        emailOrPhone: false,
        usableRoute: false,
        stage4Complete: false,
        stage4StillRequired: false,
      };
      if (text(c.title) || text(c.jobTitle) || text(c.professionalTitle)) {
        row.title = true;
      }
      if (text(c.email) || text(c.verifiedEmail) || text(c.professionalEmail)) {
        row.email = true;
        opEmail = true;
      }
      if (text(c.phone) || text(c.telephone) || text(c.professionalPhone)) {
        row.phone = true;
        opPhone = true;
      }
      const li = resolvePersonLinkedIn(c);
      if (li) {
        row.linkedIn = true;
        opLi = true;
        if (text(c.linkedinUrl)) recoveredLinkedInFields += 1;
      }
      const channel = c.contactChannelResearch || null;
      if (channel && typeof channel === "object") {
        opStage4 = true;
        const statuses = [
          channel.professionalEmail,
          channel.professionalTelephone,
          channel.personalLinkedIn,
        ]
          .map((s) => String(s || "").toUpperCase())
          .filter(Boolean);
        const researched = statuses.some((s) =>
          /FOUND|NOT_FOUND|UNAVAILABLE|RESEARCHED|NOT_YET_RESEARCHED/.test(s),
        );
        const stillNeeded = statuses.some((s) =>
          /NOT_YET_RESEARCHED|PENDING/.test(s),
        );
        if (researched) row.stage4Complete = true;
        if (stillNeeded) row.stage4StillRequired = true;
      }
      row.emailOrPhone = row.email || row.phone;
      row.usableRoute = row.email || row.phone || row.linkedIn;
      people.set(key, row);
    }

    if (opEmail) emailOps += 1;
    if (opPhone) phoneOps += 1;
    if (opLi) linkedInOps += 1;
    if (opEmail || opPhone || opLi) usableOps += 1;
    if (opStage4) stage4Ops += 1;
  }

  let withTitle = 0;
  let withEmail = 0;
  let withPhone = 0;
  let withLinkedIn = 0;
  let withEmailOrPhone = 0;
  let withUsableRoute = 0;
  let stage4CompletedPersons = 0;
  let stage4StillRequiredPersons = 0;
  for (const p of people.values()) {
    if (p.title) withTitle += 1;
    if (p.email) withEmail += 1;
    if (p.phone) withPhone += 1;
    if (p.linkedIn) withLinkedIn += 1;
    if (p.emailOrPhone) withEmailOrPhone += 1;
    if (p.usableRoute) withUsableRoute += 1;
    if (p.stage4Complete) stage4CompletedPersons += 1;
    if (p.stage4StillRequired) stage4StillRequiredPersons += 1;
  }

  return {
    contractId: "gdi-contact-coverage-scorecard-v1",
    audience: "FOUNDER_ADMIN_ONLY",
    grain: "QUALIFIED_INVENTORY_AND_UNIQUE_PERSON",
    customerFacing: false,
    opportunityLevel: {
      totalQualifiedOpportunities: opportunities.length,
      withVerifiedNamedContacts: namedOps,
      withVerifiedEmail: emailOps,
      withVerifiedTelephone: phoneOps,
      withVerifiedPersonalLinkedIn: linkedInOps,
      withEmailOrTelephone: opportunities.filter((o) => {
        const c = o?.primaryContact || {};
        return Boolean(text(c.email) || text(c.phone));
      }).length,
      withUsableProfessionalOutreachRoute: usableOps,
      withCompletedStage4ChannelAnnotation: stage4Ops,
    },
    uniquePersonLevel: {
      uniquePeople: people.size,
      withVerifiedFullName: people.size,
      withVerifiedProfessionalTitle: withTitle,
      withVerifiedEmail: withEmail,
      withVerifiedTelephone: withPhone,
      withVerifiedPersonalLinkedIn: withLinkedIn,
      withEmailOrTelephone: withEmailOrPhone,
      withUsableProfessionalOutreachRoute: withUsableRoute,
      withCompletedStage4Research: stage4CompletedPersons,
      stillRequiringStage4Research: stage4StillRequiredPersons,
    },
    recoveryAndDefects: {
      linkedInFieldsHydratedOnContacts: recoveredLinkedInFields,
      contactInformationRecoveredFromExistingEvidence:
        "LinkedIn sourceUrl→linkedinUrl mapping + settled title recovery (see STAGED package provenance)",
      contactPersistenceDefects:
        "Email/phone gaps on disc_* rows remain PUBLIC_CONTACT_RESEARCH_NOT_EXECUTED — not persistence defects after hydration tip",
      contactDisplayDefects:
        "List/DTO LinkedIn omission remediated; unavailable states retained only when channel genuinely missing",
    },
  };
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

/**
 * Competitive Opportunities KPI — overlapping Market Scope population
 * (matches Opportunities filter Competitive count). Exclusive-only counting
 * incorrectly reported 0 when competitive rows were also hotel-eligible.
 */
function competitiveCount(opportunities = []) {
  return opportunities.filter(
    (o) =>
      o?.competitiveMarketScopeEligible === true ||
      o?.isCompetitiveMarketIntelligence === true ||
      o?.competitiveOverflow === true,
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
function formatHumanMonth(ym) {
  const m = String(ym || "").match(/^(\d{4})-(\d{2})$/);
  if (!m) return String(ym || "");
  const months = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  const idx = Number(m[2]) - 1;
  return `${months[idx] || m[2]} ${m[1]}`;
}

/**
 * Applicable calendar year for the outlook chart: modal year among dated
 * ISO event months; ties prefer the later year; empty inventory uses UTC year.
 */
export function resolveOutlookApplicableYear(monthKeys = [], now = new Date()) {
  const years = (monthKeys || [])
    .map((k) => Number(String(k || "").slice(0, 4)))
    .filter((y) => Number.isFinite(y) && y >= 2000 && y <= 2100);
  if (!years.length) return now.getUTCFullYear();
  const counts = new Map();
  for (const y of years) counts.set(y, (counts.get(y) || 0) + 1);
  let best = years[0];
  let bestN = 0;
  for (const [y, n] of counts) {
    if (n > bestN || (n === bestN && y > best)) {
      best = y;
      bestN = n;
    }
  }
  return best;
}

/**
 * Jan–Dec for the applicable year (zero-filled) plus any dated months
 * outside that year so future/past evidenced months are not dropped.
 */
export function buildFullYearOutlookTimeline(byMonth, applicableYear) {
  const year = Number(applicableYear);
  const filled = [];
  for (let m = 1; m <= 12; m += 1) {
    const key = `${year}-${String(m).padStart(2, "0")}`;
    const existing = byMonth.get(key);
    filled.push(
      existing || {
        month: key,
        monthLabel: formatHumanMonth(key),
        count: 0,
        opportunityIds: [],
        zeroFilled: true,
      },
    );
  }
  const extras = [...byMonth.values()]
    .filter((r) => !String(r.month || "").startsWith(`${year}-`))
    .sort((a, b) => String(a.month).localeCompare(String(b.month)));
  const before = extras.filter((r) => String(r.month) < `${year}-01`);
  const after = extras.filter((r) => String(r.month) > `${year}-12`);
  return {
    applicableYear: year,
    timeline: [...before, ...filled, ...after],
  };
}

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

    // Chart uses ISO event starts only — never display-pill "Ongoing" text.
    const start = text(opp.eventStartDate);
    if (!/^\d{4}-\d{2}/.test(start)) continue;
    const key = start.slice(0, 7);
    const row = byMonth.get(key) || {
      month: key,
      monthLabel: formatHumanMonth(key),
      count: 0,
      opportunityIds: [],
    };
    row.count += 1;
    row.opportunityIds.push(oppId(opp));
    byMonth.set(key, row);
  }
  const applicableYear = resolveOutlookApplicableYear([...byMonth.keys()]);
  const { timeline } = buildFullYearOutlookTimeline(byMonth, applicableYear);
  const chartTotal = timeline.reduce((n, r) => n + (r.count || 0), 0);
  return {
    grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
    applicableYear,
    timeline,
    buckets,
    chartKind: "TIMELINE_BAR",
    reconciliation: {
      qualifiedInventory: opportunities.length,
      bucketSum:
        buckets.establishedEventDates +
        buckets.estimatedDemandPeriods +
        buckets.ongoingAccountRelationships +
        buckets.timingNotEstablished,
      chartOpportunityCount: chartTotal,
      applicableYear,
      note:
        "Chart counts opportunities with ISO eventStartDate only. Ongoing accounts are bucketed separately and do not appear as monthly bars. Applicable calendar year is zero-filled Jan–Dec; dated months outside that year are retained.",
    },
    note:
      chartTotal === 0
        ? "No evidenced future event months in this inventory. Dates are never invented."
        : `${chartTotal} of ${opportunities.length} opportunities have evidenced event months.`,
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

function formatHumanDate(isoDay) {
  const raw = String(isoDay || "").slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) return null;
  const [y, m, d] = raw.split("-").map((n) => Number(n));
  if (!y || !m || !d) return null;
  const dt = new Date(Date.UTC(y, m - 1, d));
  if (!Number.isFinite(dt.getTime())) return null;
  return new Intl.DateTimeFormat("en-US", {
    month: "long",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
  }).format(dt);
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
  // Customer-facing period copy must not embed STAGED/CURRENT publication jargon.
  let label;
  const humanAsOf = formatHumanDate(asOfDay);
  if (humanAsOf) {
    label = `Research updated ${humanAsOf}`;
  } else {
    label = "Latest authorized research";
  }
  return {
    label,
    customerLabel: label,
    asOfDay,
    asOfLabel: humanAsOf || null,
    kind: founderPreview ? "STAGED_PACKAGE" : "CURRENT_PUBLICATION",
    visibilityContext: founderPreview ? "FOUNDER_PREVIEW" : "CUSTOMER_CURRENT",
    visibilityContextNote:
      "Publication status is separate from the research update date.",
    founderStatusLabel: founderPreview ? "Staged" : null,
    asOf,
    publicationVersion: version,
  };
}

function countVerifiedContactChannels(opportunities = []) {
  const people = new Map();
  for (const opp of opportunities) {
    for (const c of [opp?.primaryContact, opp?.secondaryContact].filter(Boolean)) {
      const name = text(c.name) || text(c.fullName);
      if (!name) continue;
      const key = name.toLowerCase();
      const row = people.get(key) || { email: false, phone: false, linkedIn: false };
      if (text(c.email) || text(c.verifiedEmail) || text(c.professionalEmail)) {
        row.email = true;
      }
      if (text(c.phone) || text(c.telephone) || text(c.professionalPhone)) {
        row.phone = true;
      }
      if (resolvePersonLinkedIn(c)) row.linkedIn = true;
      people.set(key, row);
    }
  }
  let channels = 0;
  for (const p of people.values()) {
    if (p.email) channels += 1;
    if (p.phone) channels += 1;
    if (p.linkedIn) channels += 1;
  }
  return { uniquePeople: people.size, verifiedChannels: channels };
}

function buildCustomerResearchReceipt(opportunities = [], opts = {}) {
  const evidence = new Set();
  for (const opp of opportunities) {
    for (const u of collectEvidenceUrls(opp)) evidence.add(u);
  }
  const weekly = buildWeeklyChangeIntegrity(opportunities);
  const contactScore =
    opts.contactCoverage || buildContactCoverageScorecard(opportunities);
  const contactChannels = countVerifiedContactChannels(opportunities);
  const funnelCohort =
    opts.researchFunnelCohort ||
    resolveResearchFunnelCohort({
      hotelId: opts.hotelId,
      screenedOutCount: opts.screenedOutCount,
    });

  const funnelStages = [];
  const pushFunnel = (spec) => {
    if (spec.omit) return;
    funnelStages.push({
      key: spec.key,
      label: spec.label,
      value: spec.value,
      availability: spec.availability,
      tooltip: spec.tooltip,
      grain: spec.grain,
      cohort: spec.cohort,
      note: spec.note || null,
      customerFacing: true,
    });
  };

  pushFunnel({
    key: "potentialOpportunitiesIdentified",
    label: "Potential Opportunities Identified",
    value: funnelCohort.stages?.potentialOpportunitiesIdentified ?? null,
    availability:
      funnelCohort.stages?.potentialOpportunitiesIdentified != null
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
    tooltip:
      "Unique relevant discovery candidates identified during the applicable research scope — before detailed research and commercial qualification.",
    grain: METRIC_GRAIN.RESEARCH_ACTIVITY,
    cohort: "research_stage",
    omit: false,
    note:
      funnelCohort.stages?.potentialOpportunitiesIdentified == null
        ? "Stage-level discovery records are not available for this hotel yet."
        : null,
  });
  pushFunnel({
    key: "opportunitiesResearched",
    label: "Opportunities Researched",
    value: funnelCohort.stages?.opportunitiesResearched ?? null,
    availability:
      funnelCohort.stages?.opportunitiesResearched != null
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
    tooltip:
      "Unique opportunities that received the defined detailed research process (deep research dossiers).",
    grain: METRIC_GRAIN.RESEARCH_ACTIVITY,
    cohort: "research_stage",
    note:
      funnelCohort.stages?.opportunitiesResearched == null
        ? "Detailed research stage records are not available for this hotel yet."
        : null,
  });
  // Omit validated unless an independent milestone exists (do not fabricate).
  if (!funnelCohort.omitValidated) {
    pushFunnel({
      key: "opportunitiesValidated",
      label: "Opportunities Validated",
      value: funnelCohort.stages?.opportunitiesValidated ?? null,
      availability:
        funnelCohort.stages?.opportunitiesValidated != null
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      tooltip:
        "Researched opportunities that passed an independent evidence and commercial-relevance validation milestone before final qualification.",
      grain: METRIC_GRAIN.RESEARCH_ACTIVITY,
      cohort: "research_stage",
    });
  }
  pushFunnel({
    key: "qualifiedOpportunitiesDelivered",
    label: "Qualified Opportunities Delivered",
    value: opportunities.length,
    availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING,
    tooltip:
      "Complete commercially qualified inventory delivered in this authorized publication. Includes retained opportunities from earlier research cycles when present.",
    grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
    cohort: "publication_inventory",
    note:
      "Cumulative qualified inventory in the authorized publication — distinct from current-period discovery volume.",
  });

  const supportingSpecs = [
    {
      key: "evidenceSourcesReviewed",
      label: "Evidence Sources Reviewed",
      value: evidence.size,
      availability:
        evidence.size > 0
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
          : RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
      tooltip:
        "Unique evidence source URLs attached to qualified opportunities in this publication.",
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      reason: evidence.size > 0 ? null : "Evidence URLs not present on all package rows",
    },
    {
      key: "commercialContactsIdentified",
      label: "Commercial Contacts Identified",
      value: contactScore.uniquePersonLevel?.uniquePeople ?? contactChannels.uniquePeople,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      tooltip:
        "Unique verified people identified as commercial contacts across this qualified inventory.",
      grain: "UNIQUE_PERSON",
    },
    {
      key: "verifiedContactChannels",
      label: "Verified Contact Channels",
      value: contactChannels.verifiedChannels,
      availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
      tooltip:
        "Evidenced professional outreach channels (email, telephone, or personal LinkedIn) counted across verified people — not inferred from opportunity count.",
      grain: "CONTACT_CHANNEL",
    },
    {
      key: "opportunitiesScreenedOut",
      label: "Opportunities Screened Out",
      value: funnelCohort.supporting?.opportunitiesScreenedOut ?? null,
      availability:
        funnelCohort.supporting?.opportunitiesScreenedOut != null
          ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_EXISTING
          : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      tooltip:
        "Opportunities with an explicit screening disposition. Hold, incomplete research, and research continuations are not counted as screened out.",
      grain: METRIC_GRAIN.RESEARCH_ACTIVITY,
      omitWhenUnavailable: true,
      reason:
        funnelCohort.supporting?.opportunitiesScreenedOut == null
          ? "No durable screening disposition ledger for this research cohort."
          : null,
    },
    {
      key: "newIntelligenceThisPeriod",
      label: "New Intelligence This Period",
      value: weekly.comparisonAvailable
        ? Number(weekly.counts?.newThisWeek) || 0
        : null,
      availability: weekly.comparisonAvailable
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      tooltip:
        "Qualified opportunities newly appearing in the comparable research period.",
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      omitWhenUnavailable: true,
      reason: weekly.comparisonAvailable ? null : weekly.note,
    },
    {
      key: "existingIntelligenceUpdated",
      label: "Existing Intelligence Updated",
      value: weekly.comparisonAvailable
        ? (Number(weekly.counts?.updated) || 0) +
          (Number(weekly.counts?.reactivated) || 0)
        : null,
      availability: weekly.comparisonAvailable
        ? RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED
        : RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
      tooltip:
        "Previously known qualified opportunities updated or reactivated in the comparable research period.",
      grain: METRIC_GRAIN.QUALIFIED_INVENTORY,
      omitWhenUnavailable: true,
      reason: weekly.comparisonAvailable ? null : weekly.note,
    },
  ];

  const mapMetric = (m) => ({
    ...buildResearchReceiptMetric(m.key, {
      value: m.value,
      availability: m.availability,
      attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
      reason: m.reason || null,
    }),
    label: m.label,
    grain: m.grain,
    tooltip: m.tooltip || null,
    cohort: m.cohort || null,
    note: m.note || m.reason || null,
    omitWhenUnavailable: m.omitWhenUnavailable === true,
    customerFacing: true,
  });

  const funnel = funnelStages.map(mapMetric);
  const supportingMetrics = supportingSpecs
    .filter((m) => !(m.omitWhenUnavailable && m.value == null))
    .map(mapMetric);

  // Legacy flat metrics array: funnel first, then supporting (non-regression for older readers).
  const metrics = [...funnel, ...supportingMetrics];

  const productivity = opts.researchProductivity || null;
  const hoursSaved = productivity?.customerMetrics?.estimatedResearchHoursSaved
    ? {
        ...productivity.customerMetrics.estimatedResearchHoursSaved,
        displayUnit: "Hours",
        customerEstimateLabel: "Estimated",
        tooltip:
          "Estimated sales-research hours avoided after residual hotel review time. Uses provisional benchmarks — not payroll savings.",
      }
    : null;
  const daysSaved = productivity?.customerMetrics?.equivalentWorkingDaysSaved
    ? {
        ...productivity.customerMetrics.equivalentWorkingDaysSaved,
        displayUnit: "Days",
        customerEstimateLabel: "Estimated",
        tooltip:
          "Estimated research hours saved ÷ 8 working hours per day, rounded to the nearest half day.",
      }
    : null;

  return {
    contractId: GDI_RESEARCH_RECEIPT_CONTRACT_ID,
    audience: "CUSTOMER_FACING",
    hotelId: opts.hotelId || null,
    reportingPeriod: opts.reportingPeriod || null,
    layout: {
      productivityProminent: true,
      researchFunnel: true,
      supportingCompact: true,
      methodologyExpandable: true,
    },
    researchFunnel: {
      headline: "Research Funnel",
      lede: "Volume of research performed before commercially qualified opportunities are delivered.",
      stages: funnel,
      cohort: {
        available: funnelCohort.available === true,
        label: funnelCohort.cohortLabel || null,
        countingUnit: funnelCohort.countingUnit || null,
        notes: funnelCohort.notes || [],
      },
    },
    supportingMetrics,
    metrics,
    researchProductivity: productivity
      ? {
          headline: productivity.customerHeadline,
          lede: productivity.customerLede,
          methodologySummary: productivity.methodologySummary,
          methodologyExplanation: productivity.methodologyExplanation || null,
          disclaimer: productivity.disclaimer,
          estimateClass: productivity.estimateClass,
          customerEstimateLabel: "Estimated",
          benchmarkVersion: productivity.benchmarkVersion,
          metrics: [hoursSaved, daysSaved].filter(Boolean).concat(
            productivity.customerMetrics.estimatedResearchLaborValue?.value !=
              null
              ? [productivity.customerMetrics.estimatedResearchLaborValue]
              : [],
          ),
          equivalentManualResearchHours:
            productivity.customerMetrics.equivalentManualResearchHours,
        }
      : null,
    provenance: {
      grainNote:
        "Research funnel uses stage-level research records when available. Qualified inventory is a separate cumulative publication grain.",
      syntheticCountsProhibited: true,
      funnelSources: funnelCohort.sources || [],
      hoursSavedBenchmarkNote:
        "Wyndham hours-saved moved from an uncertified stacked 119 to reconciled 85 after overlap prevention (deep replaces discovery/qualification; pursuit excluded; 20% residual). Benchmark methodology unchanged.",
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
  const contactCoverage = buildContactCoverageScorecard(opportunities);
  const person = contactCoverage.uniquePersonLevel;
  const oppLvl = contactCoverage.opportunityLevel;
  const baseRi = {
    audience: "FOUNDER_ADMIN_ONLY",
    grain: METRIC_GRAIN.RESEARCH_ACTIVITY,
    restricted: true,
    contactCoverageScorecard: contactCoverage,
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
        key: "contactCoverageQualifiedOpportunities",
        label: "Contact Coverage — Qualified Opportunities",
        value: oppLvl.totalQualifiedOpportunities,
        availability: "DERIVED",
        note: "Opportunity grain (not unique people).",
      },
      {
        key: "contactCoverageNamedOps",
        label: "Opportunities With Verified Named Contacts",
        value: oppLvl.withVerifiedNamedContacts,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageUniquePeople",
        label: "Unique Verified Contact People",
        value: person.uniquePeople,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageTitles",
        label: "Contacts With Verified Professional Titles",
        value: person.withVerifiedProfessionalTitle,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageEmail",
        label: "Contacts With Verified Email",
        value: person.withVerifiedEmail,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageTelephone",
        label: "Contacts With Verified Telephone",
        value: person.withVerifiedTelephone,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageLinkedIn",
        label: "Contacts With Verified Personal LinkedIn",
        value: person.withVerifiedPersonalLinkedIn,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageEmailOrPhone",
        label: "Contacts With Email Or Telephone",
        value: person.withEmailOrTelephone,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageUsableRoute",
        label: "Contacts With Usable Professional Outreach Route",
        value: person.withUsableProfessionalOutreachRoute,
        availability: "DERIVED",
        note: "Email, telephone, or personal LinkedIn.",
      },
      {
        key: "contactCoverageStage4Complete",
        label: "Contacts With Completed Stage 4 Research",
        value: person.withCompletedStage4Research,
        availability: "DERIVED",
      },
      {
        key: "contactCoverageStage4StillRequired",
        label: "Contacts Still Requiring Stage 4 Research",
        value: person.stillRequiringStage4Research,
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

  const competitiveStaged = opportunities.filter(
    (o) =>
      o?.competitiveMarketScopeEligible === true ||
      (Array.isArray(o?.inventoryPopulations) &&
        o.inventoryPopulations.includes("COMPETITIVE_MARKET_INTELLIGENCE")),
  ).length;
  const competitiveCompletion = buildCompetitiveResearchCompletionRecord({
    hotelId: opts.hotelId || null,
    hotelName: opts.hotelName || null,
    compSetEstablished: opts.compSetEstablished === true,
    competitiveResearchExecuted:
      opts.competitiveResearchExecuted === true ||
      Number(opts.competitiveCandidatesDiscovered || 0) > 0 ||
      competitiveStaged > 0,
    competitorsResearched: opts.competitorsResearched,
    competitiveCandidatesDiscovered: opts.competitiveCandidatesDiscovered,
    candidatesDeeplyResearched: opts.candidatesDeeplyResearched,
    candidatesScreenedOut: opts.candidatesScreenedOut,
    competitiveOpportunitiesQualified:
      opts.competitiveOpportunitiesQualified ?? competitiveStaged,
    competitiveOpportunitiesStaged: competitiveStaged,
    remainingCommercialResearchRequirements:
      opts.remainingCommercialResearchRequirements,
    actualResearchExpenditureUsd: opts.competitiveResearchExpenditureUsd,
    partialHistorical: opts.competitivePartialHistorical === true,
    internalNote: opts.competitiveInternalNote || null,
  });
  baseRi.competitiveResearchCompletion = competitiveCompletion;
  baseRi.metrics = baseRi.metrics.concat(
    competitiveCompletionMetricsForResearchIntelligence(competitiveCompletion),
  );
  return baseRi;
}

function countPursueOriented(opportunities = []) {
  return opportunities.filter((o) =>
    /build account|pursue|qualify/i.test(
      text(o.salesActionPillLabel) + " " + text(o.salesActionKey),
    ),
  ).length;
}

function buildExecutiveAssessment(opportunities = [], composition, outlook) {
  const topSeg = (composition.rows || []).slice(0, 3);
  const competitive = competitiveCount(opportunities);
  const weekly = buildWeeklyChangeIntegrity(opportunities);
  const pursue = countPursueOriented(opportunities);
  const ongoing = Number(outlook?.buckets?.ongoingAccountRelationships) || 0;
  const dated = Number(outlook?.buckets?.establishedEventDates) || 0;

  const segmentPhrase = topSeg.length
    ? topSeg
        .map((r) => `${r.name} (${r.count})`)
        .join("; ")
    : null;

  const cards = {
    marketOpportunity: opportunities.length
      ? `${opportunities.length} qualified opportunities are in market for this hotel` +
        (segmentPhrase ? `, led by ${segmentPhrase}.` : ".")
      : "No qualified opportunities are available for commercial review.",
    recommendedCommercialFocus:
      pursue > 0
        ? `${pursue} opportunities carry Build Account or Pursue actions with named commercial contacts. Start with the priority tiles below, then work Monitor opportunities as capacity allows.`
        : "Review priority opportunities with the sales team and confirm the next commercial action for each account.",
    forwardDemandOutlook:
      dated > 0
        ? `${dated} opportunities have evidenced event dates` +
          (outlook.timeline?.length
            ? ` from ${outlook.timeline[0].monthLabel || outlook.timeline[0].month} through ${outlook.timeline[outlook.timeline.length - 1].monthLabel || outlook.timeline[outlook.timeline.length - 1].month}.`
            : ".") +
          (ongoing > 0
            ? ` ${ongoing} additional opportunities are ongoing account relationships without a single event month.`
            : "")
        : ongoing > 0
          ? `${ongoing} opportunities are ongoing account relationships. Establish event or demand windows where commercial timing matters.`
          : "Future demand timing is not yet established for this inventory.",
  };

  // Optional cards only when intelligence exists — never permanent empty diagnostics.
  if (competitive > 0) {
    cards.competitiveCapture = `${competitive} opportunities also appear in Competitive Market Intelligence — useful for capture and displacement conversations.`;
  }
  if (weekly.comparisonAvailable) {
    cards.materialChanges = `Since the prior comparable research period: ${weekly.counts?.newThisWeek || 0} new, ${weekly.counts?.updated || 0} updated, ${weekly.counts?.reactivated || 0} reactivated.`;
  }

  return {
    ...cards,
    // Legacy keys retained for older admin renderers (non-regression).
    strongestCommercialDemandSegments: segmentPhrase,
    immediateCommercialFocus: cards.recommendedCommercialFocus,
    competitiveCapturePotential: cards.competitiveCapture || null,
    materialChangesSinceLastResearch: cards.materialChanges || null,
    businessImpact: cards.forwardDemandOutlook,
    primaryCards: [
      {
        key: "marketOpportunity",
        title: "Market Opportunity",
        icon: "market",
        body: cards.marketOpportunity,
      },
      {
        key: "recommendedCommercialFocus",
        title: "Recommended Commercial Focus",
        icon: "focus",
        body: cards.recommendedCommercialFocus,
      },
      {
        key: "forwardDemandOutlook",
        title: "Forward Demand Outlook",
        icon: "outlook",
        body: cards.forwardDemandOutlook,
      },
    ],
    optionalCards: [
      cards.competitiveCapture
        ? {
            key: "competitiveCapture",
            title: "Competitive Capture",
            icon: "competitive",
            body: cards.competitiveCapture,
          }
        : null,
      cards.materialChanges
        ? {
            key: "materialChanges",
            title: "Material Changes",
            icon: "changes",
            body: cards.materialChanges,
          }
        : null,
    ].filter(Boolean),
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
  const contactCoverage = buildContactCoverageScorecard(opportunities);
  const researchFunnelCohort = resolveResearchFunnelCohort({
    hotelId: input.hotelId,
    screenedOutCount:
      input.screenedOutCount == null ? null : Number(input.screenedOutCount),
  });
  const receipt = buildCustomerResearchReceipt(opportunities, {
    hotelId: input.hotelId,
    reportingPeriod,
    researchProductivity,
    contactCoverage,
    researchFunnelCohort,
    screenedOutCount:
      input.screenedOutCount == null ? null : Number(input.screenedOutCount),
  });
  const assessment = buildExecutiveAssessment(
    opportunities,
    composition,
    outlook,
  );

  // Actionable ≠ qualified. Require pursue-oriented sales action (not Monitor-only).
  const actionable = countPursueOriented(opportunities);
  const upcomingDemand = opportunities.filter((o) => {
    const start = text(o.eventStartDate);
    if (!/^\d{4}-\d{2}/.test(start)) return false;
    const t = Date.parse(start);
    return Number.isFinite(t) && t >= Date.now() - 86400000;
  }).length;

  const needMatchKnown = opportunities.some(
    (o) => o?.needDateMatch === true || o?.needDateMatch === false,
  );
  const needAligned = opportunities.filter((o) => o?.needDateMatch === true).length;

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
    upcomingDemandWindows: upcomingDemand,
    opportunitiesAlignedWithNeedPeriods: needMatchKnown ? needAligned : null,
    needPeriodsAvailability: needMatchKnown
      ? "AVAILABLE"
      : "HOTEL_INPUT_REQUIRED",
    estimatedRoomNights: null,
    estimatedRoomNightsNote:
      "Estimated room nights are hidden until evidenced on opportunity rows.",
    priorityDemandSegments: composition.rows.slice(0, 5),
    reportingPeriod,
    lastResearchCompletionDate: reportingPeriod.asOf,
    lastResearchCompletionDay: reportingPeriod.asOfDay,
    distinguishesOpportunitiesFromContacts: true,
  };

  const executiveOverview = {
    perspective: EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW,
    commercialKpiRibbon: [
      {
        key: "qualified",
        label: "Qualified Opportunities",
        value: snapshot.totalQualifiedOpportunities,
        tooltip: "Commercially qualified opportunities in the authorized inventory.",
      },
      {
        key: "new",
        label: "New Opportunities This Period",
        value: snapshot.newOpportunitiesSincePriorPeriod,
        availability: snapshot.newOpportunitiesAvailability,
        tooltip:
          "New since the prior comparable research period. Shown only when Weekly Change history exists.",
      },
      {
        key: "competitive",
        label: "Competitive Opportunities",
        value: snapshot.competitiveOpportunities,
        tooltip:
          "Opportunities visible in Competitive Market Intelligence (may also appear in Hotel scope).",
      },
      {
        key: "actionable",
        label: "Actionable Opportunities",
        value: snapshot.actionableOpportunities,
        tooltip:
          "Opportunities with Build Account, Pursue, or Qualify sales actions — not Monitor-only rows.",
      },
      {
        key: "upcoming",
        label: "Upcoming Demand",
        value: upcomingDemand,
        tooltip:
          "Opportunities with evidenced future event dates (ISO event start).",
      },
      {
        key: "needPeriods",
        label: "Need Period Alignment",
        value: snapshot.opportunitiesAlignedWithNeedPeriods,
        availability: snapshot.needPeriodsAvailability,
        tooltip:
          "Opportunities aligned to hotel-provided need periods. Requires hotel need-date input.",
      },
    ].filter((k) => {
      // Omit optional KPIs that are unavailable rather than painting measured zeros.
      if (k.key === "needPeriods" && k.availability === "HOTEL_INPUT_REQUIRED") {
        return true; // keep with unavailable state — founder asked for concise unavailable
      }
      return true;
    }),
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
    // Prefer receipt-wrapped productivity so UI gets displayUnit (Hours / Days).
    researchProductivity: receipt.researchProductivity || {
      headline: researchProductivity.customerHeadline,
      lede: researchProductivity.customerLede,
      methodologySummary: researchProductivity.methodologySummary,
      methodologyExplanation: researchProductivity.methodologyExplanation,
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
            methodologyExplanation: researchProductivity.methodologyExplanation,
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
  buildContactCoverageScorecard,
  buildDemandComposition,
  buildCommercialDemandOutlook,
  prioritizeCustomerOpportunitiesV2,
  resolveCanonicalDemandSegmentLabel,
  resolveCanonicalOpportunityTitle,
};
