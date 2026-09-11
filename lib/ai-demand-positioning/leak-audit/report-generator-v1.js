/**
 * Generate commercial one-page AI Demand Leak Audit report fields.
 * Owner-readable in under two minutes. Cautious language only.
 */

import { FREE_AUDIT_SCOPE } from "./schema-v1.js";

export const WHO_DOES_THE_WORK = Object.freeze({
  title: "How We Partner on These Improvements",
  subtitle:
    "A clear path from the priorities above to public updates — Dealality readies the work, you stay in control, and we stay with you through the next monitoring cycle.",
  dealalityPrepares: [
    "Source audits",
    "Gap analysis",
    "Draft copy",
    "Competitor comparison",
    "Action checklists",
  ],
  dealalityPreparesShort:
    "We assemble audits, draft copy, and checklists so you can move quickly",
  hotelApproves: [
    "Facts",
    "Claims",
    "Final language",
    "Publishing decisions",
  ],
  hotelApprovesShort:
    "You approve facts, claims, and final wording before anything goes live",
  hotelOrAgencyPublishes: [
    "Website",
    "OTAs",
    "Google Business Profile",
    "TripAdvisor",
  ],
  hotelOrAgencyPublishesShort:
    "Your team or agency publishes on Website, OTAs, GBP, and TripAdvisor",
  dealalityMonitors: [
    "Whether signals change",
    "Whether competitors continue to appear instead",
  ],
  dealalityMonitorsShort:
    "We re-check signals, competitors, and progress on the next run",
});

export const FORBIDDEN_CAUSATION_PATTERNS = Object.freeze([
  /\bthis proves\b/i,
  /\bai is penalizing\b/i,
  /\bfixing this will increase visibility\b/i,
  /\bthis will improve ranking\b/i,
  /\bguaranteed\b/i,
  /\bwill increase bookings\b/i,
]);

export const REQUIRED_CAUTIOUS_PATTERNS = Object.freeze([
  /the evidence suggests/i,
  /this may indicate/i,
  /a likely issue to review/i,
]);

function visibilityBand(mentionRate) {
  if (mentionRate >= 0.55) return "strong";
  if (mentionRate >= 0.25) return "mixed";
  return "weak";
}

function countBy(arr, keyFn) {
  const map = new Map();
  for (const item of arr) {
    const k = keyFn(item);
    if (!k) continue;
    map.set(k, (map.get(k) || 0) + 1);
  }
  return [...map.entries()].sort((a, b) => b[1] - a[1]);
}

function strongTerritory(obs) {
  const mentioned = obs.filter((o) => o.subjectHotelMentioned);
  const ranked = countBy(mentioned, (o) => o.demandTerritory || "general demand");
  return ranked[0]?.[0] || "leisure and stay discovery";
}

function buildCompetitorDisplacementBlocks(obs, topCompetitors) {
  const blocks = [];
  for (const [name] of topCompetitors.slice(0, 2)) {
    const related = obs.filter(
      (o) =>
        o.displacedCompetitorName === name ||
        (Array.isArray(o.competitorsMentioned) && o.competitorsMentioned.includes(name))
    );
    const displacementCount = related.filter((o) => o.displacedByCompetitor).length;
    const segment =
      countBy(related, (o) => o.demandTerritory || "unspecified demand")[0]?.[0] ||
      "selected demand segments";
    const excerptObs =
      related.find((o) => o.displacedByCompetitor && o.aiResponseExcerpt) ||
      related.find((o) => o.aiResponseExcerpt);
    blocks.push({
      competitorName: name,
      demandSegment: segment,
      displacementCount,
      evidenceExcerpt: String(excerptObs?.aiResponseExcerpt || "")
        .replace(/\s+/g, " ")
        .trim()
        .slice(0, 220),
    });
  }
  return blocks;
}

function buildFixes(hotelName, topLeakTerritory) {
  const segment = topLeakTerritory;
  const segmentLower = String(segment).toLowerCase();

  return [
    {
      title: `Clarify the ${segmentLower} offer`,
      whatToUpdate: `Public positioning and package details for ${segment}`,
      whereToUpdate: "Hotel website (dedicated page or clear section), primary OTAs, and Google Business Profile",
      whoNeedsToApprove: "Hotel owner / operator (commercial and brand claims)",
      dealalityCanPrepare: [
        "Current source audit",
        "Comparison against displaced competitors",
        "Draft website copy",
        "Suggested OTA / Google Business Profile language",
        "Completion checklist",
      ],
      hotelMustConfirm: [
        "Capacity and offer specifics for this demand segment",
        "Event / amenity facts that may be claimed",
        "Package details and inclusions",
        "Final approved wording",
      ],
      summary: `Update the ${segmentLower} offer on the hotel website and mirror the same facts on OTAs and Google Business Profile.`,
    },
    {
      title: "Reconcile inconsistent public descriptions",
      whatToUpdate: "Positioning language and amenity claims across channels",
      whereToUpdate: "Website, OTAs, Google Business Profile, and major review platforms",
      whoNeedsToApprove: "Hotel owner / operator or appointed agency",
      dealalityCanPrepare: [
        "Side-by-side source inconsistency list",
        "Recommended canonical description",
        "Channel-by-channel edit checklist",
      ],
      hotelMustConfirm: [
        "Which description is the source of truth",
        "Any claims that must not appear publicly",
        "Final language before publishing",
      ],
      summary:
        "Align website, OTA, and Google Business Profile descriptions so public sources tell one coherent story about the hotel.",
    },
    {
      title: "Publish a short evidence pack for AI-readable sources",
      whatToUpdate: "Fact sheet plus review-response templates for recurring misconceptions",
      whereToUpdate: "Website download / press or groups page, and review-platform response workflows",
      whoNeedsToApprove: "Hotel owner / operator (facts and tone)",
      dealalityCanPrepare: [
        "Draft fact sheet",
        "Competitor comparison notes",
        "Suggested review-response templates",
        "Evidence package outline",
      ],
      hotelMustConfirm: [
        "Verified facts and capacities",
        "Approved claims and tone",
        "Publishing decisions for each channel",
      ],
      summary: `Create a downloadable fact sheet for ${hotelName} and response templates so key facts are easier for AI systems and guests to find.`,
    },
  ];
}

/**
 * @returns commercial report content for storage + client payload
 */
export function generateLeakAuditReportContent({ hotelName, observations, providersUsed }) {
  const obs = Array.isArray(observations) ? observations : [];
  const total = Math.max(obs.length, 1);
  const mentioned = obs.filter((o) => o.subjectHotelMentioned).length;
  const mentionRate = mentioned / total;
  const band = visibilityBand(mentionRate);

  const displacementObs = obs.filter((o) => o.displacedByCompetitor);
  const leakTerritories = countBy(
    displacementObs,
    (o) => o.demandTerritory || "unspecified demand"
  );
  const competitors = countBy(
    obs.flatMap((o) => {
      if (o.displacedCompetitorName) return [o.displacedCompetitorName];
      return o.competitorsMentioned || [];
    }),
    (c) => c
  );

  const topLeakTerritory = leakTerritories[0]?.[0] || "selected demand segments";
  const strongArea = strongTerritory(obs);
  const topCompetitor = competitors[0]?.[0] || null;
  const secondCompetitor = competitors[1]?.[0] || null;
  const competitorBlocks = buildCompetitorDisplacementBlocks(obs, competitors);

  const bottomLineSummary = topCompetitor
    ? `AI is currently recognizing ${hotelName} for ${strongArea}, but visibility appears weaker for ${topLeakTerritory}. ` +
      `In the monitored sample, ${topCompetitor} appeared in scenarios where ${hotelName} was absent. ` +
      `This suggests a possible demand leak that should be reviewed before the hotel invests further in this segment.`
    : `AI visibility for ${hotelName} looks ${band} in this limited diagnostic sample. ` +
      `Recognition appears relatively stronger for ${strongArea}, while ${topLeakTerritory} looks thinner. ` +
      `This suggests a possible demand gap that should be reviewed before the hotel invests further in that segment.`;

  const biggestDemandLeak = topCompetitor
    ? `${topLeakTerritory} demand appears to be leaking to ${topCompetitor} in monitored AI answers.`
    : `${topLeakTerritory} demand visibility appears thinner than expected for ${hotelName} in monitored AI answers.`;

  const mainCompetitorShowingUpInstead = topCompetitor
    ? secondCompetitor
      ? `${topCompetitor} (primary) and ${secondCompetitor}`
      : topCompetitor
    : "No single dominant competitor displaced the hotel in this sample.";

  const likelyReason =
    `The evidence suggests public signals for ${hotelName} may be incomplete or less clear for ${topLeakTerritory}. ` +
    `This may indicate gaps on the hotel website, OTAs, or review platforms. ` +
    `A likely issue to review is whether positioning language and package details are consistent across major public sources. ` +
    `The hotel may need stronger public evidence around ${String(topLeakTerritory).toLowerCase()} offers and amenities.`;

  const fixes = buildFixes(hotelName, topLeakTerritory);

  const recommendedNextStep =
    "Book your live ADP walkthrough this week.\n\n" +
    "This limited diagnostic shows where AI demand may be leaking. In one focused session, " +
    "Dealality will open the full AI Demand Positioning view for your hotel: the dashboard, the supporting evidence, " +
    "and which improvements can move first.\n\n" +
    "If useful, the next step is to schedule a live ADP walkthrough and determine whether a paid pilot is appropriate. " +
    "Paid ADP pilots include monthly monitoring, action playbooks, assisted execution support, and review meetings.";

  return {
    bottomLineSummary,
    biggestDemandLeak,
    mainCompetitorShowingUpInstead,
    competitorDisplacement: competitorBlocks,
    likelyReason,
    firstFix: fixes[0].summary,
    secondFix: fixes[1].summary,
    thirdFix: fixes[2].summary,
    fixes,
    whoDoesTheWork: WHO_DOES_THE_WORK,
    recommendedNextStep,
    meta: {
      band,
      mentioned,
      totalObservations: obs.length,
      topLeakTerritory,
      strongArea,
      topCompetitor,
      providersUsed: providersUsed || [],
      scopeNote: `Limited free audit: max ${FREE_AUDIT_SCOPE.maxObservations} observations`,
      limitedDiagnostic: true,
    },
  };
}

export function assertCautiousReportLanguage(text) {
  const blob = String(text || "");
  const forbiddenHits = FORBIDDEN_CAUSATION_PATTERNS.filter((re) => re.test(blob)).map(String);
  const hasCautious = REQUIRED_CAUTIOUS_PATTERNS.some((re) => re.test(blob));
  return {
    ok: forbiddenHits.length === 0 && hasCautious,
    forbiddenHits,
    hasCautious,
  };
}
