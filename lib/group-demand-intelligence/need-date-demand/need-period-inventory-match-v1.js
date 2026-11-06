/**
 * Match existing GDI inventory to hotel need periods BEFORE paid discovery.
 * Never creates new opportunity IDs for known records — association only.
 */

import {
  NEED_DATE_DEMAND_INTELLIGENCE_V1,
  NEED_OUTPUT_CATEGORY,
  NEED_TIMING_OVERLAP,
  NEED_REVENUE_TYPE,
} from "./need-date-demand-contract-v1.js";
import { evaluateNeedPeriodTimingOverlap } from "./need-period-timing-match-v1.js";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function oppRevenueTags(opp = {}) {
  const tags = [];
  const push = (v) => {
    const t = text(v).toUpperCase().replace(/\s+/g, "_");
    if (t) tags.push(t);
  };
  if (Array.isArray(opp.revenueTypeTags)) opp.revenueTypeTags.forEach(push);
  if (Array.isArray(opp.hotelRevenueTypes)) opp.hotelRevenueTypes.forEach(push);
  const blob = [
    opp.opportunityType,
    opp.demandFamily,
    opp.researchSegment,
    opp.detailLayout,
    opp.preferredLodgingPartnershipV1 === true ? "GROUP_GUESTROOMS" : "",
  ]
    .map(text)
    .join(" ")
    .toLowerCase();

  if (/\bcatering|day meeting|breakfast|lunch\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.LOCAL_CATERING);
  }
  if (/\bmeeting|conference|congress|summit\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.MEETING_SPACE);
  }
  if (/\bguestroom|room block|overnight|lodging|group\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.GROUP_GUESTROOMS);
  }
  if (/\bextended.?stay|relocation\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.EXTENDED_STAY);
  }
  if (/\bwedding\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.WEDDING_GROUP_GUESTROOMS);
  }
  if (/\bresort\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.RESORT_GROUP);
  }
  if (/\bprivate dining|banquet\b/.test(blob)) {
    tags.push(NEED_REVENUE_TYPE.PRIVATE_DINING);
    tags.push(NEED_REVENUE_TYPE.BANQUETS);
  }
  if (!tags.length) tags.push(NEED_REVENUE_TYPE.OTHER_ANCILLARY);
  return [...new Set(tags)];
}

function revenueFit(needPeriod, opportunity) {
  const need = new Set(needPeriod.revenueNeed || []);
  // Hotel-reported need months may omit revenue streams until sales clarifies.
  // Unspecified ≠ mismatch: allow timing match across inventory revenue types.
  if (!need.size) {
    const tags = oppRevenueTags(opportunity);
    return {
      fit: true,
      matched: tags,
      reason: "need_revenue_unspecified_accept_all",
      tags,
    };
  }
  const tags = oppRevenueTags(opportunity);
  const matched = tags.filter((t) => need.has(t));
  // Soft fit: meetings need can match group+meeting opps
  if (!matched.length) {
    if (
      need.has(NEED_REVENUE_TYPE.MEETING_SPACE) &&
      tags.includes(NEED_REVENUE_TYPE.GROUP_GUESTROOMS)
    ) {
      return {
        fit: true,
        matched: [NEED_REVENUE_TYPE.GROUP_GUESTROOMS],
        reason: "soft_group_to_meeting_fit",
      };
    }
    return { fit: false, matched: [], reason: "revenue_type_mismatch", tags };
  }
  return { fit: true, matched, reason: "revenue_overlap", tags };
}

function isArchived(opp = {}) {
  return (
    opp.archived === true ||
    opp.prospectPublicationWithdrawn === true ||
    String(opp.archiveState || "").toUpperCase() === "ARCHIVED" ||
    String(opp.customerFacingState || "").toUpperCase() === "ARCHIVED"
  );
}

/**
 * Score / classify one opportunity against one need period.
 */
export function matchOpportunityToNeedPeriod(needPeriod, opportunity = {}) {
  const id = opportunity.id || opportunity.opportunityId;
  const timing = evaluateNeedPeriodTimingOverlap(needPeriod, opportunity);
  const revenue = revenueFit(needPeriod, opportunity);
  const archived = isArchived(opportunity);

  let category = null;
  let actionable = false;
  let reason = timing.reason;

  if (archived) {
    reason = "archived_identity_dedup_only";
  } else if (!revenue.fit) {
    reason = revenue.reason;
  } else if (
    timing.overlapStatus === NEED_TIMING_OVERLAP.OUTSIDE_PERIOD
  ) {
    reason = timing.reason;
  } else if (
    timing.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP ||
    timing.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP ||
    timing.overlapStatus === NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL ||
    timing.overlapStatus === NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED
  ) {
    category = NEED_OUTPUT_CATEGORY.EXISTING_MATCH;
    actionable =
      timing.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP ||
      timing.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP;
  }

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    opportunityId: id,
    title: opportunity.title || opportunity.organizationName || null,
    hotelId: opportunity.hotelId || needPeriod.hotelId,
    needPeriodId: needPeriod.needPeriodId,
    category,
    outputCategory: category,
    archived,
    reactivationForbidden: archived,
    revenue,
    timing,
    actionable,
    researchGapHints: buildResearchGapHints(timing, revenue, opportunity),
    association: category
      ? {
          type: "need_period_link",
          opportunityId: id,
          needPeriodId: needPeriod.needPeriodId,
          overlapStatus: timing.overlapStatus,
          preservesCanonicalId: true,
        }
      : null,
    reason,
  };
}

function buildResearchGapHints(timing, revenue, opportunity) {
  const gaps = [];
  if (
    timing.overlapStatus === NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED ||
    timing.overlapStatus === NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL
  ) {
    gaps.push("VERIFY_NEED_PERIOD_DEMAND_DATES");
  }
  if (
    timing.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP
  ) {
    gaps.push("CONFIRM_DAY_PRECISION_OR_EDITION");
  }
  if (!opportunity.primaryContact && !opportunity.contactName) {
    gaps.push("NAMED_BUYER_OR_REFERRAL");
  }
  if (!revenue.fit) gaps.push("REVENUE_TYPE_ALIGNMENT");
  return gaps;
}

/**
 * Match full inventory to one or more need periods.
 * @param {object[]} needPeriods
 * @param {object[]} opportunities
 */
export function matchInventoryToNeedPeriods(needPeriods = [], opportunities = []) {
  const periods = Array.isArray(needPeriods) ? needPeriods : [];
  const opps = Array.isArray(opportunities) ? opportunities : [];
  const matches = [];
  const byOpportunity = new Map();

  for (const period of periods) {
    for (const opp of opps) {
      const m = matchOpportunityToNeedPeriod(period, opp);
      if (!m.category && !m.archived) continue;
      if (!m.category && m.archived) {
        // Keep archived only for dedup ledger, not as existing match output
        matches.push({ ...m, dedupOnly: true });
        continue;
      }
      matches.push(m);
      const list = byOpportunity.get(m.opportunityId) || [];
      list.push(m);
      byOpportunity.set(m.opportunityId, list);
    }
  }

  const existingMatches = matches.filter(
    (m) => m.category === NEED_OUTPUT_CATEGORY.EXISTING_MATCH && !m.dedupOnly,
  );
  const confirmed = existingMatches.filter(
    (m) =>
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.CONFIRMED_PERIOD_OVERLAP,
  );
  const probable = existingMatches.filter(
    (m) =>
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.SUPPORTED_PROBABLE_OVERLAP,
  );
  const ongoing = existingMatches.filter(
    (m) =>
      m.timing?.overlapStatus === NEED_TIMING_OVERLAP.ONGOING_ACCOUNT_POTENTIAL,
  );
  const unconfirmed = existingMatches.filter(
    (m) => m.timing?.overlapStatus === NEED_TIMING_OVERLAP.TIMING_UNCONFIRMED,
  );

  const multiPeriodLinks = [...byOpportunity.entries()]
    .filter(([, list]) => list.filter((x) => x.category).length > 1)
    .map(([opportunityId, list]) => ({
      opportunityId,
      needPeriodIds: list.map((x) => x.needPeriodId),
      note: "Same canonical opportunity linked to multiple need periods — not duplicated",
    }));

  return {
    version: NEED_DATE_DEMAND_INTELLIGENCE_V1,
    inventoryCount: opps.length,
    needPeriodCount: periods.length,
    existingMatchCount: existingMatches.length,
    confirmedCount: confirmed.length,
    probableCount: probable.length,
    ongoingAccountCount: ongoing.length,
    timingUnconfirmedCount: unconfirmed.length,
    archivedDedupCount: matches.filter((m) => m.dedupOnly).length,
    multiPeriodLinks,
    matches: existingMatches,
    archivedForDedup: matches.filter((m) => m.dedupOnly),
    exclusionIds: [
      ...new Set(opps.map((o) => o.id || o.opportunityId).filter(Boolean)),
    ],
  };
}
